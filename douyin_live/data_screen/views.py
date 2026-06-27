#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一认证与权限路由模块
"""

from django.shortcuts import render, redirect

# ============================================================
# 0. 着陆页 (无需登录)
# ============================================================
def landing_view(request):
    """全屏 Hero 着陆页，引导用户进入控制台"""
    # 已登录用户直接跳过着陆页
    if request.user.is_authenticated:
        return redirect('portal')
    return render(request, 'registration/landing.html')
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.contrib import messages

from data_screen.models import LiveRoom


# ============================================================
# 1. 全局登录视图 (智能路由)
# ============================================================
def user_login(request):
    """
    GET  → 渲染登录页
    POST → 认证用户，成功后统一重定向到门户页 portal_index
    """
    if request.method == 'GET':
        return render(request, 'registration/login.html')

    # ---- POST 处理 ----
    username = request.POST.get('username', '').strip()
    password = request.POST.get('password', '')
    error = None

    if not username or not password:
        error = '请输入用户名和密码'
    else:
        user = authenticate(request, username=username, password=password)
        if user is None:
            error = '用户名或密码错误，请重试'
        else:
            login(request, user)
            # 所有用户统一跳转到门户页
            return redirect('portal')

    return render(request, 'registration/login.html', {'error': error})


# ============================================================
# 2. 注册视图 (仅创建普通用户)
# ============================================================
def user_register(request):
    """
    GET  → 渲染注册页
    POST → 校验输入 → 创建普通用户 (is_staff=False, is_superuser=False)
           → 自动登录 → 重定向到大屏
    """
    if request.method == 'GET':
        return render(request, 'registration/register.html')

    # ---- POST 处理 ----
    username = request.POST.get('username', '').strip()
    email = request.POST.get('email', '').strip()
    password = request.POST.get('password', '')
    confirm_password = request.POST.get('confirm_password', '')

    errors = {}

    if not username:
        errors['username'] = '请输入用户名'
    elif User.objects.filter(username=username).exists():
        errors['username'] = '该用户名已被注册'

    if not email:
        errors['email'] = '请输入邮箱'
    elif User.objects.filter(email=email).exists():
        errors['email'] = '该邮箱已被注册'

    if not password:
        errors['password'] = '请输入密码'
    elif len(password) < 6:
        errors['password'] = '密码长度不能少于 6 位'

    if password != confirm_password:
        errors['confirm_password'] = '两次输入的密码不一致'

    if errors:
        return render(request, 'registration/register.html', {
            'errors': errors,
            'username': username,
            'email': email,
        })

    # 创建普通用户（非管理员）
    user = User.objects.create_user(
        username=username,
        email=email,
        password=password,
    )
    user.is_staff = False
    user.is_superuser = False
    user.save()

    # 自动登录 → 普通用户直接进大屏
    login(request, user)
    messages.success(request, f'欢迎，{username}！')

    return redirect('dashboard')


# ============================================================
# 3. 登出视图
# ============================================================
def user_logout(request):
    """退出登录，清除 session → 返回登录页"""
    logout(request)
    return redirect('login')


# ============================================================
# 4. 管理员门户视图 (需登录)
# ============================================================
@login_required
def portal_index(request):
    """
    管理员中转门户：双卡片导航
    - 前往数据大屏 /dashboard/
    - 前往管理后台 /admin/
    """
    return render(request, 'registration/portal.html')


# ============================================================
# 5. 数据大屏视图 (需登录)
# ============================================================
@login_required
def dashboard_view(request):
    """数据大屏入口页面，附带最近监控的直播间"""
    recent_rooms = LiveRoom.objects.order_by('-updated_at')[:5]
    return render(request, 'data_screen/dashboard.html', {
        'recent_rooms': recent_rooms,
    })
