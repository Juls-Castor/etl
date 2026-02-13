from rest_framework.views import APIView
from rest_framework.response import Response
from django.views.generic import TemplateView
import plotly.graph_objects as go

from django.db import connection
import pdb


class CustomersView(APIView):
    """/api/sells/customers?top=N"""

    def get(self, request):
        top = int(request.GET.get("top", 5))
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT c.customer_name, SUM(f.total) as facturacion
                FROM fact_invoices f
                JOIN dim_customer c ON f.customer_key = c.customer_key
                GROUP BY c.customer_name
                ORDER BY facturacion DESC
                LIMIT %s
            """,
                [top],
            )
            result = [
                {"customer": row[0], "sells": float(row[1])}
                for row in cursor.fetchall()
            ]
        return Response(result)


class SalesTimeView(APIView):
    """/api/sells/time?period=monthly|weekly&start=YYYY-MM-DD&end=YYYY-MM-DD"""

    def get(self, request):
        period = request.GET.get("period", "monthly")
        start = request.GET.get("start")
        end = request.GET.get("end")

        with connection.cursor() as cursor:
            if period == "monthly":
                cursor.execute(
                    """
                    SELECT DATE_FORMAT(issue_date,'%%Y-%%m') as period, SUM(total)
                    FROM fact_invoices
                    WHERE issue_date BETWEEN %s AND %s
                    GROUP BY period
                    ORDER BY period
                """,
                    [start, end],
                )
            elif period == "weekly":
                cursor.execute(
                    """
                    SELECT YEAR(issue_date), WEEK(issue_date), SUM(total)
                    FROM fact_invoices
                    WHERE issue_date BETWEEN %s AND %s
                    GROUP BY YEAR(issue_date), WEEK(issue_date)
                    ORDER BY YEAR(issue_date), WEEK(issue_date)
                """,
                    [start, end],
                )

            result = [
                {"period": str(row[0]) + " - W" + str(row[1]), "sells": float(row[2])}
                for row in cursor.fetchall()
            ]

        return Response(result)


class DashboardView(TemplateView):
    template_name = "app/dashboard.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        # =========================
        # 
        # =========================
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT DATE_FORMAT(issue_date,'%Y-%m') as period, SUM(total) as monthly_total
                FROM fact_invoices
                GROUP BY DATE_FORMAT(issue_date, '%Y-%m')
                ORDER BY period
            """)
            sales_data = cursor.fetchall()

        periods = [row[0] for row in sales_data]
        monthly = [float(row[1] or 0) for row in sales_data]

        cumulative = []
        running = 0.0
        for v in monthly:
            running += v
            cumulative.append(running)

        fig_sales = go.Figure()
        fig_sales.add_trace(
            go.Bar(
                x=periods,
                y=monthly,
                name="Monthly Sales",
                hovertemplate="Month: %{x}<br>Sales: $%{y:,.2f}<extra></extra>",
            )
        )

        fig_sales.add_trace(
            go.Scatter(
                x=periods,
                y=cumulative,
                name="Accumulative",
                mode="lines+markers",
                yaxis="y2",
                hovertemplate="Accumulative: $%{y:,.2f}<extra></extra>",
            )
        )


        fig_sales.update_layout(
            title="Sales Historic Trend (Monthly vs Accumulative)",
            template="plotly_white",
            hovermode="x unified",
            barmode="group",
            xaxis=dict(title="Month"),
            yaxis=dict(
                title="Total sales per month",
                tickprefix="$",
                separatethousands=True,
                showgrid=True,
            ),
            yaxis2=dict(
                title="Accumulative",
                tickprefix="$",
                separatethousands=True,
                overlaying="y",
                side="right",
                showgrid=False,
            ),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
            margin=dict(l=20, r=20, t=60, b=40),
            height=420,
        )

        fig_sales.update_xaxes(tickangle=-35)

        # =========================
        # 2) Pendiente por cliente
        # =========================
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT c.customer_name, SUM(f.total) as pendiente
                FROM fact_invoices f
                JOIN dim_customer c ON f.customer_key = c.customer_key
                WHERE f.status_key = (SELECT status_key FROM dim_status WHERE status_name='PENDING')
                GROUP BY c.customer_name
                ORDER BY pendiente DESC
                LIMIT 15
            """)
            pending_data = cursor.fetchall()

        customers = [row[0] for row in pending_data]
        pendings = [float(row[1]) for row in pending_data]

        fig_pending = go.Figure()
        fig_pending.add_trace(go.Bar(x=customers, y=pendings, name="Pending"))
        fig_pending.update_layout(
            title="Pending Customers",
            xaxis_title="Customer",
            yaxis_title="Doubt",
            template="plotly_white",
            height=360,
            margin=dict(l=20, r=20, t=60, b=80),
        )
        fig_pending.update_xaxes(tickangle=-35)

        # Pasar ambos HTMLs al template
        context["sales_plot_html"] = fig_sales.to_html(full_html=False)
        context["pending_plot_html"] = fig_pending.to_html(full_html=False)

        return context