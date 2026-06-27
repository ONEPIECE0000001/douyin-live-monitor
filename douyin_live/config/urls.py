from django.contrib import admin
from django.urls import path
from data_screen import api_views, views

urlpatterns = [
    # ---- 着陆页 & 登录 ----
    path('', views.landing_view, name='landing'),
    path('login/', views.user_login, name='login'),

    # ---- 注册 & 登出 ----
    path('accounts/register/', views.user_register, name='register'),
    path('accounts/logout/', views.user_logout, name='logout'),

    # ---- 管理员门户 (中转页) ----
    path('portal/', views.portal_index, name='portal'),

    # ---- Django 管理后台 ----
    path('admin/', admin.site.urls),

    # ---- 数据大屏入口 ----
    path('dashboard/', views.dashboard_view, name='dashboard'),

    # ---- 数据大屏 API 接口 ----
    path('api/stats/<str:room_id>/',        api_views.stats,      name='api_stats'),
    path('api/charts/trend/<str:room_id>/',   api_views.trend,      name='api_trend'),
    path('api/charts/gift_rank/<str:room_id>/', api_views.gift_rank, name='api_gift_rank'),
    path('api/charts/sentiment/<str:room_id>/', api_views.sentiment, name='api_sentiment'),

    # ---- AI 流量预测接口 ----
    path('api/predict/<str:room_id>/', api_views.predict, name='api_predict'),

]
