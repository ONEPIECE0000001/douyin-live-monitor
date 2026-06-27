from django.db import models
from django.utils import timezone
from decimal import Decimal


class LiveRoom(models.Model):
    room_id = models.CharField(
        max_length=50, unique=True, primary_key=True,
        verbose_name='房间ID'
    )
    host_name = models.CharField(
        max_length=100,
        verbose_name='主播昵称'
    )
    host_id = models.CharField(
        max_length=50, blank=True, null=True,
        verbose_name='主播ID'
    )
    viewer_count = models.IntegerField(
        default=0,
        verbose_name='观看人数'
    )
    like_count = models.BigIntegerField(
        default=0,
        verbose_name='点赞数'
    )
    created_at = models.DateTimeField(
        auto_now_add=True,
        verbose_name='开始监控时间'
    )
    updated_at = models.DateTimeField(
        auto_now=True,
        verbose_name='监控结束时间'
    )
    total_likes = models.BigIntegerField(
        default=0,
        verbose_name='累计点赞'
    )
    total_gifts_value = models.DecimalField(
        max_digits=15, decimal_places=2, default=Decimal('0.00'),
        verbose_name='累计礼物价值(元)'
    )
    current_online = models.IntegerField(
        default=0,
        verbose_name='当前在线人数'
    )

    @property
    def total_duration_str(self):
        """
        动态计算所有关联场次的累计监测时长。
        已结束场次：end_time - start_time
        进行中场次：timezone.now() - start_time
        返回人性化格式：X小时Y分 / Y分
        """
        sessions = self.livesession_set.all()
        total_seconds = 0
        now = timezone.now()

        for session in sessions:
            if session.end_time:
                total_seconds += (session.end_time - session.start_time).total_seconds()
            else:
                total_seconds += (now - session.start_time).total_seconds()

        hours = int(total_seconds // 3600)
        minutes = int((total_seconds % 3600) // 60)

        if hours > 0:
            return f"{hours}小时{minutes}分"
        return f"{minutes}分"

    class Meta:
        db_table = 'live_room'
        verbose_name = '直播间信息'
        verbose_name_plural = '直播间信息'

    def __str__(self):
        return f"{self.host_name} - {self.room_id}"


class LiveSession(models.Model):
    session_id = models.CharField(
        max_length=50, unique=True, primary_key=True,
        verbose_name='场次ID'
    )
    room = models.ForeignKey(
        LiveRoom, on_delete=models.CASCADE, db_column='room_id',
        verbose_name='直播间'
    )
    start_time = models.DateTimeField(
        default=timezone.now,
        verbose_name='监控开始时间'
    )
    end_time = models.DateTimeField(
        blank=True, null=True,
        verbose_name='监控结束时间'
    )
    total_danmu = models.BigIntegerField(
        default=0,
        verbose_name='弹幕总量'
    )
    peak_online = models.IntegerField(
        default=0,
        verbose_name='峰值在线人数'
    )

    class Meta:
        db_table = 'live_session'
        verbose_name = '直播场次统计'
        verbose_name_plural = '直播场次统计'
        indexes = [
            models.Index(fields=['room', 'start_time']),
        ]

    def __str__(self):
        return f"Session {self.session_id} - {self.room.host_name}"
