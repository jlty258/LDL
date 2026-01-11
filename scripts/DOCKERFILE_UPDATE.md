# Dockerfile 更新说明

## 更新内容

已将 Python uber 镜像的 Dockerfile 从硬编码依赖改为从 `requirements.txt` 文件安装依赖。

## 变更详情

### 之前（硬编码依赖）
```dockerfile
pip install \
    requests>=2.31.0 \
    mysql-connector-python>=8.2.0 \
    psycopg2-binary>=2.9.9 \
    apache-dolphinscheduler>=3.0.0
```

### 现在（从文件安装）
```dockerfile
COPY requirements.txt /tmp/requirements.txt
...
pip install -r /tmp/requirements.txt
```

## requirements.txt 内容

`scripts/requirements.txt` 现在包含所有依赖：

```
requests>=2.31.0
mysql-connector-python>=8.2.0
psycopg2-binary>=2.9.9
apache-dolphinscheduler>=3.0.0
```

## 优势

1. **易于维护**：所有依赖集中在一个文件中管理
2. **版本控制**：依赖版本变更可以通过 Git 追踪
3. **可复用**：其他项目可以直接使用相同的 requirements.txt
4. **一致性**：确保开发环境和生产环境使用相同的依赖版本

## 使用方法

### 添加新依赖

1. 编辑 `scripts/requirements.txt`，添加新依赖：
   ```
   new-package>=1.0.0
   ```

2. 重新构建镜像：
   ```bash
   cd scripts
   docker-compose build
   ```

### 更新依赖版本

1. 编辑 `scripts/requirements.txt`，修改版本号
2. 重新构建镜像

### 验证依赖安装

```bash
cd scripts
docker-compose run --rm python-scripts pip list
```

## 注意事项

- `requirements.txt` 必须在 Dockerfile 的构建上下文中（与 Dockerfile 同一目录）
- 修改 `requirements.txt` 后需要重新构建镜像才能生效
- 建议使用精确的版本号或版本范围，避免依赖冲突
