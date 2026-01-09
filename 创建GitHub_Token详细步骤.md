# 创建GitHub Personal Access Token - 详细步骤

## 方法1：通过设置页面创建（推荐）

### 步骤详解：

1. **打开Token设置页面**
   - 在浏览器中访问：**https://github.com/settings/tokens**
   - 或者：
     - 点击右上角头像
     - 选择 "Settings"（设置）
     - 在左侧菜单找到 "Developer settings"（开发者设置）
     - 点击 "Personal access tokens"
     - 选择 "Tokens (classic)"

2. **创建新Token**
   - 点击绿色的 **"Generate new token"** 按钮
   - 选择 **"Generate new token (classic)"**

3. **填写Token信息**
   - **Note（备注）**: 输入 `LDL项目推送` 或任何你喜欢的名称
   - **Expiration（有效期）**: 
     - 建议选择 **90 days** 或 **No expiration**（无过期）
     - 如果选择无过期，请确保妥善保管token
   - **Select scopes（选择权限）**:
     - ✅ 勾选 **`repo`** - 这会自动勾选所有仓库相关权限
     - 这是推送代码所需的最小权限

4. **生成Token**
   - 滚动到页面底部
   - 点击绿色的 **"Generate token"** 按钮

5. **复制并保存Token**
   - ⚠️ **重要**: Token只会显示一次！
   - 立即复制token（一串类似 `ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx` 的字符串）
   - 保存到安全的地方（密码管理器、文本文件等）

## 方法2：使用SSH密钥（替代方案）

如果你不想使用token，可以配置SSH密钥：

### 检查是否已有SSH密钥：
```powershell
# 检查SSH密钥
ls $env:USERPROFILE\.ssh\id_rsa.pub
```

### 如果没有SSH密钥，创建新的：
```powershell
# 生成SSH密钥（将your_email@example.com替换为你的邮箱）
ssh-keygen -t ed25519 -C "jlty258@126.com"

# 按提示操作（可以直接按回车使用默认路径和空密码）
```

### 添加SSH密钥到GitHub：
1. 复制公钥内容：
```powershell
Get-Content $env:USERPROFILE\.ssh\id_ed25519.pub | Set-Clipboard
```

2. 在GitHub上添加SSH密钥：
   - 访问：https://github.com/settings/keys
   - 点击 "New SSH key"
   - Title: 输入 `LDL项目` 或任何名称
   - Key: 粘贴刚才复制的公钥
   - 点击 "Add SSH key"

3. 修改Git远程URL为SSH格式：
```powershell
cd D:\LDL
git remote set-url origin git@github.com:jlty258/LDL.git
git push -u origin main
```

## 使用Token推送代码

创建token后，使用以下任一方法：

### 方法A：使用批处理文件（最简单）
双击运行 `push_to_github.bat`，然后输入你的token

### 方法B：使用PowerShell脚本
```powershell
cd D:\LDL
powershell -ExecutionPolicy Bypass -File scripts/setup_github_token.ps1 -Token YOUR_TOKEN
```

### 方法C：手动命令
```powershell
cd D:\LDL
git remote set-url origin https://jlty258:YOUR_TOKEN@github.com/jlty258/LDL.git
git push -u origin main
```

## 常见问题

**Q: Token在哪里创建？**
A: https://github.com/settings/tokens → Generate new token → Generate new token (classic)

**Q: 需要哪些权限？**
A: 只需要 `repo` 权限即可

**Q: Token过期了怎么办？**
A: 重新创建一个新token，然后更新远程URL

**Q: 可以同时使用多个token吗？**
A: 可以，每个token可以有不同的权限和用途

**Q: Token泄露了怎么办？**
A: 立即在 https://github.com/settings/tokens 删除该token，然后创建新的

## 安全提示

- ⚠️ 永远不要将token提交到代码仓库
- ⚠️ 不要在公共场合分享token
- ⚠️ 定期检查并删除不需要的token
- ⚠️ 使用密码管理器保存token
