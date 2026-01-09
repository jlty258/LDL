#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GitHub Personal Access Token 创建脚本
注意：GitHub已不再支持通过API使用密码直接创建token
此脚本提供两种方式：
1. 通过GitHub API尝试（可能需要两步验证）
2. 提供手动创建指南
"""

import requests
import base64
import json
import sys

def create_token_via_api(username, password, token_name="LDL项目推送"):
    """
    尝试通过GitHub API创建Personal Access Token
    注意：GitHub已不再支持此方式，需要两步验证或特殊权限
    """
    url = "https://api.github.com/authorizations"
    
    # 使用Basic Auth
    credentials = f"{username}:{password}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()
    
    headers = {
        "Authorization": f"Basic {encoded_credentials}",
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "LDL-Project"
    }
    
    data = {
        "scopes": ["repo"],
        "note": token_name,
        "note_url": "https://github.com/jlty258/LDL"
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        
        if response.status_code == 201:
            token_data = response.json()
            token = token_data.get("token")
            print(f"✅ Token创建成功！")
            print(f"Token: {token}")
            print(f"\n⚠️  请立即保存此token，它只会显示一次！")
            return token
        elif response.status_code == 401:
            print("❌ 认证失败。可能的原因：")
            print("   1. 用户名或密码错误")
            print("   2. 账户启用了两步验证（2FA）")
            print("   3. GitHub已不再支持通过API使用密码创建token")
        elif response.status_code == 422:
            print("❌ Token创建失败。可能已存在同名的token。")
        else:
            print(f"❌ 请求失败，状态码: {response.status_code}")
            print(f"响应: {response.text}")
            
    except Exception as e:
        print(f"❌ 发生错误: {str(e)}")
    
    return None

def print_manual_guide():
    """打印手动创建Token的指南"""
    print("\n" + "="*60)
    print("📝 手动创建Personal Access Token指南")
    print("="*60)
    print("\n由于GitHub安全策略，请按以下步骤手动创建token：")
    print("\n1. 访问: https://github.com/settings/tokens")
    print("2. 点击 'Generate new token' → 'Generate new token (classic)'")
    print("3. 填写信息：")
    print("   - Note: LDL项目推送")
    print("   - Expiration: 选择有效期（建议90天或更长）")
    print("   - 勾选权限: repo (完整仓库访问权限)")
    print("4. 点击 'Generate token'")
    print("5. 复制生成的token（只显示一次，请立即保存）")
    print("\n创建token后，可以使用以下命令推送代码：")
    print("  git remote set-url origin https://jlty258:YOUR_TOKEN@github.com/jlty258/LDL.git")
    print("  git push -u origin main")
    print("\n" + "="*60)

def main():
    if len(sys.argv) < 3:
        print("用法: python create_github_token.py <用户名> <密码>")
        print("\n示例: python create_github_token.py jlty258@126.com your_password")
        print_manual_guide()
        sys.exit(1)
    
    username = sys.argv[1]
    password = sys.argv[2]
    
    print("正在尝试通过GitHub API创建Personal Access Token...")
    print("注意：此方法可能因GitHub安全策略而失败\n")
    
    token = create_token_via_api(username, password)
    
    if not token:
        print("\n" + "="*60)
        print("⚠️  通过API创建token失败")
        print("="*60)
        print_manual_guide()
        sys.exit(1)
    
    return token

if __name__ == "__main__":
    main()
