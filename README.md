# pangflow

## 关于pangflow

pangflow是一个工作流管理工具，主要利用CLI来调度和触发工作流。其主要技术包括Prefect、CLI、TOML和SQLite。

## 文档

设计文档如下：

+ 后续添加

算法包注释文档路径如下：

+ Doc/Sphinx

## 主要概念

+ Config (配置层)： TOML配置文件，声明式配置

+ TaskFactory，BaseTask (任务层)：创建和管理任务实例，主要技术工厂模式

+ ExecutionContext，Strategy (执行层)：封装环境执行逻辑，主要技术策略模式

+ Subject、Observer (观察者层)：采集日志和监控数据，主要技术观察者模式

## 使用了哪些技术

+ 工厂模式、策略模式、观察者模式

+ 元编程技术
  
+ 目前支持的工作流模式（基于CLI和模板文件接入pangflow）

|模式|技术|实现|是否已支持|
|:---:|:---:|:---:|:---:|
|trigger|prefect|CLI和模板文件|是<input type="checkbox" checked>|
|scheduler|prefect|CLI和模板文件|是<input type="checkbox" checked>|

## 如何获得Pangflow

pangflow采用setup打包，以wheel格式进行分发（包括Prefect环境，所有依赖已经打包进wheel文件中，使用pip自动安装）

```bash
$ pip install pangflow-0.1.1-py3-none-any.whl
```

pangflow后续会提供CLI和Web应用，以下为启动流程：

```bash
$ # 优先启动prefect环境
$ prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api" # 指定prefect api地址,terminal-1中执行
$ prefect server start # 启动prefect server，terminal-1中执行
$ prefect work-pool create --type process default-process # 创建process work pool，terminal-2中执行
$ prefect worker start -p "default-process" # 启动process worker，terminal-2中执行
$ # pangflow环境初始化及其相关操作
$ pangflow init # 初始化pangflow环境
$ pangflowctl register workflows/example-training.toml # 注册工作流
$ pangflowctl deploy my-training-workflow # 部署工作流
$ pangflowctl serve start my-training-workflow --detach
$ pangflowctl list # 查看所有工作流
```

pangflow暂不支持离线一键部署，后续会提供一键部署可执行shell-tar制作模板，过程具体可见Deploy

## License

pangflow License

## ChangeLog

<details>
<summary>点击查看 ChangeLog</summary>

### Version-0.1.1
  - 主要包括配置层、任务层、执行层、观察者层
  - TODO: 提供前端web界面
  - 使用setup_0311_win.py打包，以支持3.11.11版本的python解释器和windows平台分发部署

## BUG报告

BUG请在本项目的issues中追踪
