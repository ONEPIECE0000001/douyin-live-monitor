#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
认证模块 — 用户注册 + API 登录保护装饰器
"""

from functools import wraps

from django.shortcuts import render, redirect
from django.contrib.auth import login
from django.contrib.auth.models import User
from django.contrib import messages
from django.http import JsonResponse


# ============================================
# API 登录保护装饰器
# ============================================
def api_login_required(view_func):
    """
    API 端点专用：未登录返回 401 JSON（而非 302 重定向 HTML）。
    前端 Vue3 的 fetchAll() catch 块会捕获 401 并跳转登录页。
    """
    @wraps(view_func)
    def _wrapped_view(request, *args, **kwargs):
        if not request.user.is_authenticated:
            return JsonResponse(
                {'error': True, 'message': '请先登录'},
                status=401,
            )
        return view_func(request, *args, **kwargs)
    return _wrapped_view


# ============================================
# 用户注册视图
# ============================================
def register_view(request):
    """GET 渲染注册表单，POST 校验并创建用户、自动登录。"""
    if request.method == 'GET':
        return render(request, 'registration/register.html')

    # ---- POST 处理 ----
    username = request.POST.get('username', '').strip()
    email = request.POST.get('email', '').strip()
    password = request.POST.get('password', '')
    confirm_password = request.POST.get('confirm_password', '')

    errors = {}

    # 校验
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

    # 创建用户
    user = User.objects.create_user(
        username=username,
        email=email,
        password=password,
    )

    # 自动登录
    login(request, user)
    messages.success(request, f'欢迎，{username}！')

    return redirect('dashboard')
