from django.contrib import admin
from django.utils import timezone
from django.utils.html import format_html
from .models import LiveRoom, LiveSession


# ============================================
# LiveRoom — 直播间宏观档案（突出历史累计）
# ============================================
@admin.register(LiveRoom)
class LiveRoomAdmin(admin.ModelAdmin):
    list_display = [
        'room_id',
        'host_name',
        'current_online',
        'total_likes',
        'total_gifts_value',
        'get_total_duration',
        'session_count',
    ]
    list_filter = [
        ('created_at', admin.DateFieldListFilter),
        ('updated_at', admin.DateFieldListFilter),
    ]
    search_fields = ['room_id', 'host_name']
    ordering = ['-updated_at']
    date_hierarchy = 'created_at'
    list_per_page = 30
    readonly_fields = ['created_at', 'updated_at']
    list_select_related = True

    @admin.display(description='场次数')
    def session_count(self, obj):
        return obj.livesession_set.count()

    @admin.display(description='累计监测时长')
    def get_total_duration(self, obj):
        return obj.total_duration_str

    fieldsets = (
        ('基础信息', {
            'fields': ('room_id', 'host_name', 'host_id'),
        }),
        ('实时与累计数据', {
            'fields': ('current_online', 'total_likes', 'total_gifts_value',
                        'viewer_count', 'like_count'),
        }),
        ('时间戳', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',),
        }),
    )


# ============================================
# LiveSession — 单场流水（突出时间跨度与互动量）
# ============================================
@admin.register(LiveSession)
class LiveSessionAdmin(admin.ModelAdmin):
    list_display = [
        'session_id',
        'room',
        'start_time',
        'end_time',
        'duration',
        'peak_online',
        'total_danmu',
        'status',
    ]
    list_filter = [
        ('start_time', admin.DateFieldListFilter),
        ('end_time', admin.EmptyFieldListFilter),
    ]
    search_fields = ['session_id', 'room__room_id', 'room__host_name']
    ordering = ['-start_time']
    date_hierarchy = 'start_time'
    list_per_page = 30
    readonly_fields = ['start_time']
    list_select_related = ['room']

    @admin.display(description='持续时长')
    def duration(self, obj):
        if obj.end_time:
            delta = obj.end_time - obj.start_time
        else:
            delta = timezone.now() - obj.start_time
        hours, remainder = divmod(int(delta.total_seconds()), 3600)
        minutes, _ = divmod(remainder, 60)
        if hours > 0:
            return f'{hours}小时{minutes}分'
        return f'{minutes}分'

    @admin.display(description='状态')
    def status(self, obj):
        if obj.end_time is None:
            return format_html(
                '<span style="color:#22c55e;font-weight:600;">● 进行中</span>'
            )
        return format_html(
            '<span style="color:#6b7280;">○ 已结束</span>'
        )

    fieldsets = (
        ('场次标识', {
            'fields': ('session_id', 'room'),
        }),
        ('时间区间', {
            'fields': ('start_time', 'end_time'),
        }),
        ('互动数据', {
            'fields': ('peak_online', 'total_danmu'),
        }),
    )
