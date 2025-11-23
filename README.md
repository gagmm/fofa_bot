# FOFA 数据下载机器人 (FOFA Data Downloader Bot)
[English](https://github.com/CXK-Computer/fofa_bot/blob/main/README_English.md)
[Iran](https://raw.githubusercontent.com/CXK-Computer/fofa_bot/refs/heads/main/README_Iran.md)

**您的私人FOFA资产搜索引擎与分析助手。**

这是一个功能强大的 Telegram 机器人，它深度集成了 [FOFA](https://fofa.info) 的 API，允许您通过 Telegram 直接进行网络空间资产的搜索、下载、分析和管理。它不仅仅是一个数据拉取工具，更是一个集成了多种后处理和分析能力的一站式平台。

## 免责声明

* 项目内所涉及任何脚本、LOGO 、工作流仅为资源共享、学习参考之目的，不保证其合法性、正当性、准确性；请根据情况自行判断，切勿使用项目做任何商业用途或牟利；

* 遵循避风港原则，若有图片和内容等侵权，请在 Issues 告知，核实后删除，其版权均归原作者及其网站所有；

* 本人不对任何内容承担任何责任，包括但不限于任何内容错误导致的任何损失、损害;

* 其它人通过任何方式登陆本网站或直接、间接使用项目相关资源，均应仔细阅读本声明，一旦使用、转载项目任何相关教程或资源，即被视为您已接受此免责声明。

* 本项目内所有资源文件，禁止任何公众号、自媒体进行任何形式的转载、发布。

* 本项目涉及的数据由使用的个人或组织自行填写，本项目不对数据内容负责，包括但不限于数据的真实性、准确性、合法性。使用本项目所造成的一切后果，与本项目的所有贡献者无关，由使用的个人或组织完全承担。

* 本项目中涉及的第三方硬件、软件等，与本项目没有任何直接或间接的关系。本项目仅对部署和使用过程进行客观描述，不代表支持使用任何第三方硬件、软件。使用任何第三方硬件、软件，所造成的一切后果由使用的个人或组织承担，与本项目无关。

* 本项目中所有内容只供学习和研究使用，不得将本项目中任何内容用于违法行为，包括但不限于建立 VPS 或违反国家/地区/组织等的法律法规或相关规定的其他用途。作者对于由此引起的任何隐私泄漏或其他后果概不负责.

* 所有基于本项目源代码，进行的任何修改，为其他个人或组织的自发行为，与本项目没有任何直接或间接的关系，所造成的一切后果亦与本项目无关。

* 本项目保留随时对免责声明进行补充或更改的权利，直接或间接使用本项目内容的个人或组织，视为接受本项目的特别声明。

## ✨ 核心功能

*   **🚀 高级资产搜索**:
    *   **精准查询**: 使用 `/kkfofa` 命令执行任何 FOFA 语法查询。
    *   **预设与筛选**: 支持管理员预设常用查询，并可在执行时快速叠加地域（大洲）筛选。
    *   **多种下载模式**:
        *   **全量下载**: 快速下载1万条以内的结果。
        *   **深度追溯 (Traceback)**: 通过时间线回溯，突破1万条限制，获取理论上的全量数据。
        *   **增量更新**: 对已缓存的查询结果进行更新，只下载新增的数据，节省F点。

*   **📊 深度数据分析**:
    *   **主机画像 (`/host`)**: 获取单个IP或域名的全方位信息，包括开放端口、服务、证书、Banner等。
    *   **聚合统计 (`/stats`)**: 对任意查询进行全局聚合统计，快速洞察资产的宏观分布（如Top国家、服务、端口等）。
    *   **批量特征分析 (`/batchfind`)**: 上传IP列表（`ip:port`格式），机器人会自动查询并智能分析这批资产的共同特征，并**自动生成建议的FOFA查询语句**，是进行威胁情报分析和资产归类的利器。

*   **🛠️ 强大的后处理工具**:
    *   **存活检测**: 下载完成后可一键对结果进行端口存活检测。
    *   **子网扫描**: 对结果中的IP所在C段进行相同端口的扫描，以发现更多潜在资产。

*   **⚙️ 便捷的管理功能**:
    *   **交互式设置 (`/settings`)**: 通过菜单轻松管理API密钥、HTTP代理、查询预设等。
    *   **多API Key支持**: 支持添加多个FOFA API Key，并可在查询时指定使用，机器人也会在某个Key失效或F点不足时自动切换。
    *   **数据管理**: 支持配置的备份与恢复、查询历史回顾、以及导入已有的数据文件并与FOFA语法关联。
    *   **在线更新 (`/update`)**: 可配置更新源URL，实现一键在线更新机器人脚本并自动重启。

## 📖 准备工作

在开始之前，您需要准备以下三样东西：

1.  **一台服务器**: 一台可以7x24小时运行Python脚本的Linux服务器（或任何支持Python的PC/Mac）。最好不要在中国或其他Telegram被封锁的地区（比如伊朗）使用（由于GFW，需要配置网络代理）。
2.  **FOFA API 密钥**:
    *   您必须拥有一个 [FOFA](https://fofa.info) 个人会员或以上账户。（或者不开会员，多充点F点）
    *   登录后，在“个人中心” -> “API接口”中找到您的`Key`。
3.  **Telegram Bot Token**:
    *   在Telegram中搜索 `@BotFather` 并开始对话。
    *   发送 `/newbot` 命令。
    *   按照提示为您的机器人设置一个名字（Name）和用户名（Username，必须以`bot`结尾）。
    *   `BotFather` 会给您一长串字符，这就是您的 **Bot Token**。请妥善保管，不要泄露。

## 🚀 部署指南

即使您是新手，只需按照以下步骤操作，也能轻松部署成功。

### 步骤 1: 下载脚本

将本项目提供的最新版Python脚本（ `fofa.py`）下载到您的服务器上。
```bash
wget https://raw.githubusercontent.com/CXK-Computer/fofa_bot/refs/heads/main/fofa.py
```
也可以把requirements.txt也下载到服务器上。
```bash
wget https://raw.githubusercontent.com/CXK-Computer/fofa_bot/refs/heads/main/requirements.txt
```
你也可以直接去发行版中下载已经编译好的二进制文件。

### 步骤 2: 安装依赖

机器人依赖于一些Python库。打开服务器的终端，执行以下命令来安装它们：

```bash
pip3 install python-telegram-bot==13.15 requests "urllib3<2.0"
```
或者（如果你已经下载了requirements.txt）
```bash
pip3 install -r requirements.txt
```

*注意：我们指定了 `python-telegram-bot` 的版本为 `13.15` 以确保兼容性。*

### 步骤 3: 配置机器人

1.  在脚本所在的目录下，创建一个名为 `config.json` 的文件。
2.  将以下内容复制并粘贴到 `config.json` 文件中：

    ```json
    {
        "bot_token": "在这里粘贴你的Telegram Bot Token",
        "apis": [
            "在这里粘贴你的第一个FOFA API Key"
        ],
        "admins": [],
        "proxy": "",
        "full_mode": false,
        "public_mode": false,
        "presets": [],
        "update_url": ""
    }
    ```

3.  **修改配置文件**:
    *   `"bot_token"`: 替换为你在 **准备工作** 中从 `@BotFather` 获取的 Token。
    *   `"apis"`: 替换为你的 FOFA API Key。你可以添加多个Key，用逗号隔开，例如：`["key1", "key2"]`。
    *   `"admins"`: **这一项先留空**。当你第一次启动机器人并向它发送 `/start` 命令时，它会自动将你的Telegram用户ID添加为第一个管理员。
    *   `"proxy"`: 如果你的服务器需要通过代理才能访问Telegram（如中国，伊朗等地），请在这里填写代理地址，例如 `"http://127.0.0.1:7890"`。如果不需要，请保持为空 `""`。

### 步骤 4: 运行机器人

在终端中，使用以下命令启动机器人：

```bash
python3 fofa.py
```

如果一切正常，您会看到类似 "🚀 终极版机器人已启动..." 的日志信息。现在，您可以在Telegram中找到您的机器人并开始使用了！

### 步骤 5: 保持后台运行 (推荐)

为了让机器人在您关闭终端后也能持续运行，推荐使用 `nohup`：

```bash
nohup python3 -u fofa.py > fofa_bot_run.log 2>&1 &
```
也可以使用`screen`：
```bash
screen -S fofa
python3 fofa.py
```

以上操作会让机器人在后台运行，并将所有日志输出到 `fofa_bot_run.log` 文件中（screen不可以）。

## 📚 指令详解

以下是所有可用指令的详细说明。

---

### 🔍 资产查询

*   **/kkfofa `[key_index] <query>`**
    *   **功能**: 核心的FOFA查询指令。
    *   **用法**:
        *   不带参数 (`/kkfofa`): 如果设置了预设，会弹出预设查询菜单。
        *   带参数 (`/kkfofa domain="example.com"`): 直接执行查询。
        *   指定Key (`/kkfofa 2 app="nginx"`): 使用 `config.json` 中配置的第2个API Key进行查询。
    *   **交互流程**:
        1.  执行查询后，机器人会询问是否按大洲进行地域筛选。
        2.  接着，它会检查是否有本地缓存。
        3.  如果结果超过1万，会提示选择下载模式（全量/深度追溯）。

---

### 📊 数据分析

*   **/host `<ip|domain>`**
    *   **功能**: 获取单个目标的详细信息。
    *   **示例**: `/host 1.1.1.1` 或 `/host example.com`
    *   **输出**: 如果信息过多，会发送一个摘要，并将包含完整Banner/Header的详细报告作为文件发送。

*   **/stats `<query>`**
    *   **功能**: 对一个FOFA查询进行聚合统计。
    *   **示例**: `/stats app="Apache-Tomcat"`
    *   **输出**: 返回Top 5的国家、组织、服务、端口等统计信息。

*   **/batchfind**
    *   **功能**: 批量分析资产共性。这是本机器人的核心亮点之一。
    *   **用法**:
        1.  发送 `/batchfind` 命令。
        2.  上传一个`.txt`文件，文件内容为每行一个 `ip:port` (兼容各种复杂格式，如 `1.1.1.1:443 | ...`)。
        3.  通过菜单选择你感兴趣的分析维度（如服务、证书、标题等）。
        4.  机器人会批量查询这些资产，并生成一份包含Top特征和**建议FOFA查询语句**的报告。

---

### ⚙️ 管理与设置

*   **/settings**
    *   **功能**: 进入交互式设置菜单，可以管理：
        *   **API管理**: 查看、添加、删除FOFA API Key，切换查询模式（近一年/完整历史）。
        *   **预设管理**: 添加或删除常用的查询语句作为预设，方便快速调用。
        *   **代理设置**: 设置或清除HTTP代理。
        *   **备份与恢复**: 快速备份配置文件。
        *   **脚本更新**: 设置更新源URL。

*   **/history**
    *   **功能**: 查看最近10条查询历史记录及其缓存状态。

*   **/import**
    *   **功能**: 将一个已有的结果文件（`.txt`）与一条FOFA查询语句关联，并存入缓存。
    *   **用法**: 在Telegram中，**回复**一个你想导入的`.txt`文件，然后输入 `/import` 命令，机器人会提示你输入关联的查询语句。

*   **/backup** & **/restore**
    *   **功能**: 备份或恢复 `config.json` 配置文件。
    *   **用法**: `/backup` 会直接发送文件给你。`/restore` 会提示你上传配置文件。

---

### 💻 系统管理

*   **/update**
    *   **功能**: 如果在设置中配置了`update_url`，此命令会从该URL下载最新脚本并自动重启机器人。

*   **/getlog**
    *   **功能**: 获取机器人的运行日志文件 `fofa_bot.log`。

*   **/shutdown**
    *   **功能**: 安全地关闭机器人进程。

*   **/stop**
    *   **功能**: 紧急停止当前正在进行的数据下载任务（如深度追溯）。

*   **/cancel**
    *   **功能**: 取消当前正在进行的会话操作（如设置、导入等）。

## ❓ 常见问题 (FAQ)

1.  **机器人没反应怎么办？**
    *   检查 `config.json` 中的 `bot_token` 是否正确。
    *   检查服务器网络是否正常，是否能访问Telegram API（如果不能，请设置代理，国内不设置代理肯定是不能使用的）。
    *   查看 `fofa_bot_run.log` 日志文件，看是否有报错信息（有的话请提交lssue）。

2.  **为什么我发送命令，机器人提示我没有权限？**
    *   机器人启动后，第一个向它发送 `/start` 的用户会被自动设为管理员。请确保您的Telegram用户ID已被正确添加到 `config.json` 的 `admins` 列表中。

3.  **FOFA查询失败是什么原因？**
    *   **API Key无效**: 检查Key是否正确，或是否已过期。
    *   **F点不足**: 登录FOFA官网查看F点余额。
    *   **语法错误**: 检查您的FOFA查询语法是否正确。

## 📞 支持与反馈

- 🐛 **Bug 报告**：[GitHub Issues](https://github.com/CXK-Computer/fofa_bot/issues)
- 💡 **功能建议**：[GitHub Discussions](https://github.com/CXK-Computer/fofa_bot/discussions)  ## 📞 支持与反馈

## 🤝 贡献指南

欢迎提交 Issue 和 Pull Request！

1. Fork 本仓库
2. 创建特性分支：`git checkout -b feature/amazing-feature`
3. 提交更改：`git commit -m 'Add amazing feature'`
4. 推送分支：`git push origin feature/amazing-feature`
5. 提交 Pull Request

## ⭐ Star 星星走起
[![Stargazers over time](https://starchart.cc/CXK-Computer/fofa_bot.svg?variant=adaptive)](https://starchart.cc/CXK-Computer/fofa_bot)## Stargazers over time

## 🙏 致谢

[X-Fofa](https://github.com/sv3nbeast/X-Fofa) 提供深度追溯实现

`Github社区`
