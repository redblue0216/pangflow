#!/usr/bin/env python3
"""PangFlow 构建脚本：构建前端 + 打包 Python

用法：
    python build.py                  # 构建前端 + Python 打包
    python build.py --install        # 构建 + 打包 + 安装到当前环境
    python build.py --skip-frontend  # 仅打包（跳过前端构建）
    python build.py --clean          # 先清理产物，再构建 + 打包
    python build.py --verbose        # 显示详细输出

前置条件：
    - 已配置好 Conda 环境（Python >= 3.13.5）
    - Node.js >= 18, npm >= 9
"""

import argparse
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path


def _read_version() -> str:
    """Read version from pyproject.toml so build.py never goes stale."""
    pyproject = Path(__file__).parent / "pyproject.toml"
    text = pyproject.read_text(encoding="utf-8")
    m = re.search(r'^version\s*=\s*"([^"]+)"', text, re.M)
    return m.group(1) if m else "unknown"


VERSION = _read_version()

PROJECT_ROOT = Path(__file__).parent.resolve()
WEBUI_DIR = PROJECT_ROOT / "webui"
STATIC_DIR = PROJECT_ROOT / "pangflow" / "web" / "static"
DIST_DIR = PROJECT_ROOT / "dist"
BUILD_DIR = PROJECT_ROOT / "build"


def run(cmd: list[str], cwd: Path | None = None, verbose: bool = False) -> None:
    """执行命令，失败时退出"""
    if verbose:
        print(f"  $ {' '.join(str(c) for c in cmd)}")
    result = subprocess.run(cmd, cwd=cwd, capture_output=not verbose, text=True)
    if result.returncode != 0:
        print(f"  ❌ 命令失败: {' '.join(str(c) for c in cmd)}")
        if not verbose and result.stderr:
            print(result.stderr)
        sys.exit(1)


def check_prerequisites(verbose: bool) -> None:
    """检查前置条件"""
    print("【1/4】检查前置条件...")

    # 检查是否在项目根目录
    if not (PROJECT_ROOT / "pyproject.toml").exists():
        print(f"  ❌ 未找到 pyproject.toml，请确保在项目根目录下运行")
        sys.exit(1)

    if not (PROJECT_ROOT / "README.md").exists():
        print(f"  ❌ 未找到 README.md")
        sys.exit(1)

    if not WEBUI_DIR.exists():
        print(f"  ❌ 未找到 webui/ 目录")
        sys.exit(1)

    # 检查 uv（不存在则自动安装）
    try:
        result = subprocess.run(["uv", "--version"], capture_output=True, text=True)
        uv_version = result.stdout.strip()
    except FileNotFoundError:
        print("  ⚠️  未找到 uv，正在安装...")
        run([sys.executable, "-m", "pip", "install", "uv"], verbose=verbose)
        result = subprocess.run(["uv", "--version"], capture_output=True, text=True)
        uv_version = result.stdout.strip()
    print(f"  ✅ {uv_version}")

    # 检查 Node.js
    result = subprocess.run(["node", "--version"], capture_output=True, text=True)
    if result.returncode != 0:
        print("  ❌ 未找到 Node.js，请先安装 Node.js >= 18")
        sys.exit(1)
    node_version = result.stdout.strip().lstrip("v")
    print(f"  ✅ Node.js {node_version}")

    # 检查 npm
    result = subprocess.run(["npm", "--version"], capture_output=True, text=True)
    if result.returncode != 0:
        print("  ❌ 未找到 npm")
        sys.exit(1)
    npm_version = result.stdout.strip()
    print(f"  ✅ npm {npm_version}")

    # 检查 Python
    py_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    print(f"  ✅ Python {py_version}")
    print(f"  ✅ 当前环境: {sys.prefix}")


def clean_build_artifacts(verbose: bool) -> None:
    """清理构建产物"""
    print("【0/4】清理构建产物...")
    targets = [BUILD_DIR, DIST_DIR, PROJECT_ROOT / "pangflow.egg-info"]
    for target in targets:
        if target.exists():
            shutil.rmtree(target)
            if verbose:
                print(f"  已删除 {target}")
    print("  ✅ 清理完成")


def _inject_version_into_webui() -> None:
    """Sync version into webui package.json and index.html before building."""
    pkg = WEBUI_DIR / "package.json"
    if pkg.exists():
        text = pkg.read_text(encoding="utf-8")
        text = re.sub(r'"version":\s*"[^"]+"', f'"version": "{VERSION}"', text)
        pkg.write_text(text, encoding="utf-8")

    idx = WEBUI_DIR / "index.html"
    if idx.exists():
        text = idx.read_text(encoding="utf-8")
        text = re.sub(r'<title>.*?</title>', f'<title>PangFlow {VERSION}</title>', text)
        idx.write_text(text, encoding="utf-8")


def build_frontend(verbose: bool) -> None:
    """构建前端"""
    print("【2/4】构建前端...")
    print(f"  工作目录: {WEBUI_DIR}")

    _inject_version_into_webui()

    # npm install（如果 node_modules 不存在或 package.json 更新了）
    if not (WEBUI_DIR / "node_modules").exists():
        print("  安装 npm 依赖...")
        run(["npm", "install"], cwd=WEBUI_DIR, verbose=verbose)
    else:
        print("  node_modules 已存在，跳过 npm install")

    # npm run build
    print("  执行 npm run build...")
    run(["npm", "run", "build"], cwd=WEBUI_DIR, verbose=verbose)

    # 验证产物
    if not (STATIC_DIR / "index.html").exists():
        print(f"  ❌ 前端构建失败：{STATIC_DIR / 'index.html'} 未生成")
        sys.exit(1)

    js_files = list(STATIC_DIR.glob("assets/*.js"))
    css_files = list(STATIC_DIR.glob("assets/*.css"))
    print(f"  ✅ 前端构建完成")
    print(f"     index.html + {len(js_files)} 个 JS + {len(css_files)} 个 CSS")


def build_python(verbose: bool) -> None:
    """打包 Python"""
    print("【3/4】打包 Python...")
    print(f"  工作目录: {PROJECT_ROOT}")

    # uv build
    run(["uv", "build"], cwd=PROJECT_ROOT, verbose=verbose)

    # 验证产物
    wheels = list(DIST_DIR.glob("*.whl"))
    sdist = list(DIST_DIR.glob("*.tar.gz"))
    if not wheels:
        print("  ❌ 打包失败：未生成 wheel")
        sys.exit(1)

    print(f"  ✅ 打包完成")
    for whl in wheels:
        size_mb = whl.stat().st_size / (1024 * 1024)
        print(f"     {whl.name} ({size_mb:.1f} MB)")
    for tar in sdist:
        print(f"     {tar.name}")


def install_package(verbose: bool) -> None:
    """安装到当前环境"""
    print("【4/4】安装到当前环境...")
    wheels = sorted(DIST_DIR.glob("*.whl"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not wheels:
        print("  ❌ 未找到 wheel 文件")
        sys.exit(1)

    latest_wheel = wheels[0]
    print(f"  安装: {latest_wheel.name}")
    run([sys.executable, "-m", "pip", "install", "--force-reinstall", str(latest_wheel)], verbose=verbose)

    # 验证
    result = subprocess.run(
        [sys.executable, "-c", "import pangflow; print(pangflow.__version__)"],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        print(f"  ✅ 安装成功，版本: {result.stdout.strip()}")
    else:
        print(f"  ⚠️  安装后验证失败")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=f"PangFlow {VERSION} 构建脚本：构建前端 + 打包 Python",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
示例:
  python build.py                  # 构建前端 + 打包
  python build.py --install        # 构建 + 打包 + 安装
  python build.py --clean          # 清理 + 构建 + 打包
  python build.py --skip-frontend  # 仅打包（跳过前端构建）
  python build.py --verbose        # 显示详细输出

前置条件:
  - 已配置好 Conda 环境（Python >= 3.13.5）
  - Node.js >= 18, npm >= 9
        """
    )
    parser.add_argument(
        "--install", "-i",
        action="store_true",
        help="打包后额外执行 pip install（安装到当前环境）"
    )
    parser.add_argument(
        "--skip-frontend", "-s",
        action="store_true",
        help="跳过前端构建（假设 web/static/ 已有产物）"
    )
    parser.add_argument(
        "--clean", "-c",
        action="store_true",
        help="先清理 build/ dist/ egg-info/，再重新构建"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="显示详细命令输出"
    )
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"  PangFlow {VERSION} 构建脚本")
    print(f"{'='*60}\n")

    start_time = time.time()

    # 清理
    if args.clean:
        clean_build_artifacts(args.verbose)

    # 前置检查
    check_prerequisites(args.verbose)

    # 构建前端
    if not args.skip_frontend:
        build_frontend(args.verbose)
    else:
        print("【2/4】跳过前端构建（--skip-frontend）")
        if not (STATIC_DIR / "index.html").exists():
            print(f"  ⚠️  警告：{STATIC_DIR / 'index.html'} 不存在，打包后可能缺少前端文件")

    # 打包 Python
    build_python(args.verbose)

    # 安装
    if args.install:
        install_package(args.verbose)
    else:
        print("【4/4】跳过安装（加 --install 自动安装）")

    elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"  构建完成，耗时 {elapsed:.1f} 秒")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
