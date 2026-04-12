# 庫存查驗 / 銷售統計（Streamlit）

對外網址由 **Streamlit Community Cloud** 提供；資料只存在 **Supabase**（需自行開專案）。

## 一、Supabase（約 5 分鐘）

1. 註冊 [supabase.com](https://supabase.com/) → **New project**（等專案建好進 Dashboard）。
2. **建資料表（這步不會出現任何資料列，很正常）**
   - 左側選 **SQL Editor**（圖示像 `</>`；新版介面若在左欄最下面找不到，可先點 **Home** 再從選單進 **SQL**）。
   - **New query**，把本 repo 的 `supabase_schema.sql` **整份貼上** → 右下角 **Run**（或快捷鍵）。
   - 結果區若顯示 **`Success. No rows returned`** → 代表 `CREATE TABLE` 成功；**不是失敗**，只是建表指令本來就不回傳資料列。
   - 想確認表有建起來：左側 **Table Editor** → schema **public** → 應看得到 **`app_state`**。
3. **拿 Project URL 與 service_role（在 Supabase 網頁裡）**
   - 左下角 **齒輪 Project Settings**（或側欄底部的 **Settings**）點進去。
   - 左欄選 **Data API** 或 **API**（不同版本名稱二選一，都在 Settings 底下）。
   - 畫面上方會有 **Project URL**（長得像 `https://xxxx.supabase.co`）→ 複製到 Secrets 的 `SUPABASE_URL`。
   - 同一頁往下找 **Project API keys**：
     - **`anon` `public`**：給瀏覽器用的，**我們這個 app 不用**。
     - **`service_role` `secret`**：點 **Reveal** 才會顯示長 JWT → 複製到 `SUPABASE_SERVICE_ROLE_KEY`（**不要**貼在 GitHub issue、不要 commit）。

## 二、GitHub

1. 在 GitHub **New repository**（可設 Public，Cloud 免費版需公開 repo）。
2. 本機在專案目錄：

```bash
cd "/Users/yuanguoting/Invemtory&Sales"
git init
git add app.py persist_sales.py sales_reports.py verification.py requirements.txt supabase_schema.sql secrets.example.toml .gitignore .streamlit/config.toml README.md
git commit -m "Initial app for Streamlit Cloud"
git branch -M main
git remote add origin https://github.com/<你的帳號>/<repo名>.git
git push -u origin main
```

（若已 `git init` 過，略過 `git init`，改 `git remote` / `git push` 即可。）

## 三、Streamlit Community Cloud

1. 開 [share.streamlit.io](https://share.streamlit.io/)，用 GitHub 登入。
2. **New app** → 選你的 repo / **main** branch。
3. **Main file path** 填：`app.py` → **Deploy**。
4. App 頁面 → **⋮ → Settings → Secrets**，貼上（換成你的值）：

```toml
SUPABASE_URL = "https://xxxx.supabase.co"
SUPABASE_SERVICE_ROLE_KEY = "eyJhbGciOi..."
ADMIN_PASSWORD = "你自己設的強密碼"
```

帳號固定 **`admin`**，密碼只寫在 `ADMIN_PASSWORD`（不要 commit 進 repo）。

5. **Manage app → Reboot**（或再 deploy 一次）讓 Secrets 生效。

完成後 Cloud 會給你一個 `https://xxx.streamlit.app` 網址；需 **admin + 密碼** 才能進入上傳／看報表。

## 注意

- 這是 **單一帳密 + session**，適合小團隊；防不了會寫腳本撞密碼的攻擊，密碼請設長一點。
- 上傳單檔預設上限已調為約 200MB（見 `.streamlit/config.toml`）。

## 本機預覽（可選）

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
# 複製 secrets 到 .streamlit/secrets.toml 後：
streamlit run app.py
```
