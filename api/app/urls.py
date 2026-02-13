from django.urls import path
from .views import CustomersView, SalesTimeView
from .views import DashboardView

urlpatterns = [
    path("sells/customers/", CustomersView.as_view(), name="top-customers"),
    path("sells/time/", SalesTimeView.as_view(), name="sales-time"),
    path("visualization/dashboard/", DashboardView.as_view(), name="dashboard"),
]