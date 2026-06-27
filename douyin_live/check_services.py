import subprocess
import time
import sys
import os

# 加载 .env 文件 (standalone 脚本不会走 Django settings)
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env'))

try:
    import pymysql
    import pymongo
    PYMYSQL_AVAILABLE = True
    PYMONGO_AVAILABLE = True
except ImportError:
    PYMYSQL_AVAILABLE = False
    PYMONGO_AVAILABLE = False

MYSQL_CONFIG = {
    'host': os.environ.get('MYSQL_HOST', '127.0.0.1'),
    'port': int(os.environ.get('MYSQL_PORT', '3306')),
    'user': os.environ.get('MYSQL_USER', 'root'),
    'password': os.environ.get('MYSQL_PASSWORD', ''),
    'database': os.environ.get('MYSQL_DATABASE', 'douyin_live')
}

MONGO_CONFIG = {
    'host': 'localhost',
    'port': 27017
}

# MongoDB 默认数据目录与日志路径 — 以下为 Windows 典型安装路径示例。
# 非 Windows 用户或自定义安装路径时，请通过环境变量覆盖：
#   set MONGO_DBPATH=/var/lib/mongodb
#   set MONGO_LOGPATH=/var/log/mongodb/mongod.log
DEFAULT_MONGO_DBPATH = os.environ.get('MONGO_DBPATH', r'C:\ProgramData\MongoDB\data\db')
DEFAULT_MONGO_LOGPATH = os.environ.get('MONGO_LOGPATH', r'C:\ProgramData\MongoDB\log\mongod.log')

MYSQL_SERVICE_NAME = 'MySQL80'
MONGO_SERVICE_NAME = 'MongoDB'

def run_command(cmd):
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return -1, '', 'Command timeout'

def check_service_status(service_name):
    code, stdout, stderr = run_command(f'sc query "{service_name}"')
    if code == 0:
        if 'RUNNING' in stdout:
            return 'running'
        elif 'STOPPED' in stdout:
            return 'stopped'
        else:
            return 'unknown'
    else:
        return 'not_found'

def start_service(service_name):
    print(f'尝试启动服务: {service_name}')
    code, stdout, stderr = run_command(f'net start "{service_name}"')
    if code == 0:
        print(f'✅ 服务 {service_name} 启动成功')
        return True
    else:
        print(f'❌ 服务 {service_name} 启动失败: {stderr.strip()}')
        return False

def create_mysql_database():
    try:
        connection = pymysql.connect(
            host=MYSQL_CONFIG['host'],
            port=MYSQL_CONFIG['port'],
            user=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password'],
            connect_timeout=5
        )
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_CONFIG['database']} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        connection.close()
        print(f'✅ 数据库 {MYSQL_CONFIG["database"]} 创建成功')
        return True
    except pymysql.Error as e:
        print(f'❌ 创建数据库失败: {e}')
        return False

def check_mysql_connection():
    if not PYMYSQL_AVAILABLE:
        print('⚠️ pymysql 未安装，跳过 MySQL 连接测试')
        return True
    
    try:
        connection = pymysql.connect(
            host=MYSQL_CONFIG['host'],
            port=MYSQL_CONFIG['port'],
            user=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password'],
            database=MYSQL_CONFIG['database'],
            connect_timeout=5
        )
        connection.close()
        print('✅ MySQL 数据库连接成功')
        return True
    except pymysql.Error as e:
        error_code = e.args[0] if isinstance(e.args, tuple) else 0
        if error_code == 1049:
            print(f'⚠️ 数据库 {MYSQL_CONFIG["database"]} 不存在，尝试创建...')
            if create_mysql_database():
                return check_mysql_connection()
            else:
                return False
        else:
            print(f'❌ MySQL 数据库连接失败: {e}')
            return False

def check_mongo_connection():
    if not PYMONGO_AVAILABLE:
        print('⚠️ pymongo 未安装，跳过 MongoDB 连接测试')
        return True
    
    try:
        client = pymongo.MongoClient(
            host=MONGO_CONFIG['host'],
            port=MONGO_CONFIG['port'],
            serverSelectionTimeoutMS=5000
        )
        client.admin.command('ping')
        client.close()
        print('✅ MongoDB 数据库连接成功')
        return True
    except pymongo.errors.ConnectionFailure as e:
        print(f'❌ MongoDB 数据库连接失败: {e}')
        return False

def main():
    print('=' * 60)
    print('          数据库服务检测与启动脚本')
    print('=' * 60)
    
    all_successful = True
    
    print('\n【MySQL 服务检测】')
    mysql_status = check_service_status(MYSQL_SERVICE_NAME)
    print(f'当前状态: {mysql_status}')
    
    if mysql_status == 'stopped':
        if start_service(MYSQL_SERVICE_NAME):
            time.sleep(3)
            mysql_status = 'running'
        else:
            all_successful = False
    
    if mysql_status == 'running':
        if not check_mysql_connection():
            all_successful = False
    elif mysql_status == 'not_found':
        print(f'⚠️ 服务 {MYSQL_SERVICE_NAME} 未找到，请检查服务名称')
        all_successful = False
    
    print('\n【MongoDB 服务检测】')
    mongo_status = check_service_status(MONGO_SERVICE_NAME)
    print(f'当前状态: {mongo_status}')
    
    if mongo_status == 'stopped':
        if start_service(MONGO_SERVICE_NAME):
            time.sleep(3)
            mongo_status = 'running'
        else:
            print('尝试手动启动 MongoDB...')
            code, stdout, stderr = run_command(f'mongod --dbpath "{DEFAULT_MONGO_DBPATH}" --logpath "{DEFAULT_MONGO_LOGPATH}" --service')
            if code == 0:
                time.sleep(5)
                mongo_status = 'running'
                print('✅ MongoDB 启动成功')
            else:
                print(f'❌ MongoDB 启动失败: {stderr.strip()}')
                all_successful = False
    
    if mongo_status == 'running' or mongo_status == 'not_found':
        if not check_mongo_connection():
            all_successful = False
    
    print('\n' + '=' * 60)
    if all_successful:
        print('🎉 所有数据库服务检测通过！')
        return 0
    else:
        print('❌ 部分服务未启动或连接失败，请检查配置')
        return 1

if __name__ == '__main__':
    sys.exit(main())
