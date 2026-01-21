# config/Config.py
import json
import os
class Config:
    def __init__(self):
        # Replace with your actual Tushare token (api_key)
        self.api_key = "your_tushare_token_here"  # Critical: Get this from Tushare official website

    def load_secrets(self):
        # 配置文件路径
        secrets_path = os.path.join(os.path.dirname(__file__), "", "local.json")
        try:
            with open(secrets_path, "r", encoding="utf-8") as f:
                secrets = json.load(f)
            # 验证必要的密钥是否存在
            required_keys = ["db_password", "api_key"]
            for key in required_keys:
                if key not in secrets:
                    raise KeyError(f"缺少必要配置：{key}")
            return secrets
        except FileNotFoundError:
            raise Exception(f"配置文件不存在：{secrets_path}")
        except json.JSONDecodeError:
            raise Exception(f"配置文件格式错误：{secrets_path}")
# Create a global instance of Config for easy import
config = Config()

# import json
# import os

# # 使用
# try:
#     secrets = Config.load_secrets()
#     db_password = secrets["db_password"]
#     api_key = secrets["api_key"]
#     print(f"API Key：{api_key}")
# except Exception as e:
#     print(f"读取配置失败：{e}")