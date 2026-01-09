# 添加SSH密钥到GitHub - 详细步骤

## 你的SSH公钥

```
ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQC0OO/AmUDIHCyCD+BcPpubkX/QSS9aJQGve6OibLFk8k3Hg5+XZPhd8dPDsT6uvD0BM7c89HHVkKQ7wR7gHVpdGBLk1bDlZiXIleQ7guA2bEjxwlFUMKrmU1sSQDC9a+jHAcHbOPNzAS9RopduPSh26XEiirkxKTeFdXwMphLFQWIaWIpkKLP1j5HI4Kh6MfMqCaABpB2ftAiU5Tv0DS3+rwtyAcOahdphEBBXxXjiBckh/lLJDO2lIm97P/wjg9rwd396AaEMTYBYbkT4K7CzypS6yXW/03JK16dSMa1DfJvpc9VOPoo4hUygvhdRUf+wQBcrCraPEzomk3xpyujK3RvRTSJfFmEg+qzDWFdYWJYJRLRFC02XQQdO3IFyY54dtv40dx3AMw3bAMsJzsb6QVZ2Olpb3nftcezIvJrtmDT6l3iNcRfSAGWJRlGROPJj3Swxxd4AtTo5VDrygjbARvv2Zy7Opozz2wyFZh93SquYAK6CcbqVWoRhcDhHUjs= jrome258@outlook.com
```

## 添加步骤

### 1. 点击 "New SSH key" 按钮
在SSH keys区域右上角，点击绿色的 "New SSH key" 按钮

### 2. 填写信息

- **Title（标题）**: 
  - 输入 `LDL项目` 或 `Windows开发机` 或任何你喜欢的名称
  - 这个名称只是用来标识这个密钥的用途

- **Key type（密钥类型）**: 
  - 选择 **"Authentication Key"**（认证密钥）
  - 这是默认选项，用于Git操作认证

- **Key（密钥内容）**: 
  - 粘贴上面提供的完整SSH公钥
  - 从 `ssh-rsa` 开始，到 `jrome258@outlook.com` 结束
  - 必须是一整行，不要换行

### 3. 保存
- 点击绿色的 **"Add SSH key"** 按钮
- GitHub可能会要求你输入密码确认

### 4. 验证
添加成功后，你应该能在SSH keys列表中看到你刚添加的密钥

## 添加完成后

添加完成后，运行以下命令推送代码：

```powershell
cd D:\LDL
git push -u origin main
```

如果遇到 "Host key verification failed" 错误，运行：

```powershell
ssh-keyscan github.com >> $env:USERPROFILE\.ssh\known_hosts
git push -u origin main
```

## 注意事项

- SSH公钥可以安全地分享，只有私钥需要保密
- 一个GitHub账户可以添加多个SSH密钥
- 如果推送仍然失败，可能需要先验证GitHub的host key
