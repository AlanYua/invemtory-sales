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

**前提**：下面「選 repo」前，請先做完 **二、GitHub** 把程式 **push 上去**，否則清單裡看不到你的專案。

1. 開 [share.streamlit.io](https://share.streamlit.io/) → 用 **GitHub 登入**（或註冊綁 GitHub）。
2. **註冊／登入後**：
   - 若跳出 **Authorize streamlit**（授權 Streamlit 讀你的 GitHub repo）→ **Authorize** 按下去（不授權就選不到 repo）。
   - 進到主畫面後，右上角或中間找 **「Create app」／「New app」**（介面改版時字可能略有不同，都是「新建一個要跑的 app」）。
3. **部署表單**（介面預設常是 `master` + `streamlit_app.py`，請改掉）：
   - **Repository（儲存庫）** 要填的是 **GitHub 上的「擁有者/倉庫名」**，不是本機資料夾路徑。
     - 到瀏覽器開你的倉庫首頁，網址長這樣：`https://github.com/擁有者/倉庫名` → 你要填的就是 **`擁有者/倉庫名`**（中間一條斜線、**沒有** `https://`）。
     - 例：網址是 `https://github.com/yuanguoting/Invemtory-Sales` → 填 **`yuanguoting/Invemtory-Sales`**。
     - 表單旁若有 **「貼上 GitHub URL」**：可貼整條 `https://github.com/.../...`，它會幫你拆好。
     - **下拉是空的／找不到自己的 repo**：多半是還沒 push、或 Streamlit 沒被授權讀到該倉庫。請到 GitHub 右上角 **頭像 → Settings → Applications → Installed GitHub Apps → Streamlit Cloud**（名稱可能略有差異）→ **Configure**，把要部署的 **organization / repository 存取權** 打開；存檔後回 Streamlit 重新整理再選。
   - **Branch**：多數新 repo 是 **`main`**；若紅字「分支不存在」→ 到 GitHub 倉庫 **Code** 頁左上角分支下拉看實際名稱再填。
     - 若仍失敗：本 repo 已同時有 **`main`** 與 **`master`**（內容相同），部署表單可改試 **`master`**。
     - **應用網址（可選）** 請先**留空**用預設網址；自訂子網域有時會讓表單驗證怪掉。
     - 仍不行：GitHub **Settings → Applications → Installed GitHub Apps → Streamlit** → **Configure**，確認已勾到此倉庫；再到 Streamlit **登出再登入**，或換無痕視窗重填。
   - **Main file path**：本專案填 **`app.py`**（不是 `streamlit_app.py`，除非你有改名）。
   - 按 **Deploy**，等 build 跑完（第一次約 1～3 分鐘）。
4. 部署成功後，瀏覽器網址會變成 **`https://xxx.streamlit.app`** 這類網址（可先打開確認會出現登入頁；此時還沒設 Secrets 可能會報錯，正常）。
5. 在 **該 app 頁面**（不是你的 GitHub）：
   - 右上角 **⋮**（三點）→ **Settings** → 左欄或分頁找 **Secrets**。
   - 貼上（換成你的值）：

```toml
SUPABASE_URL = "https://xxxx.supabase.co"
SUPABASE_SERVICE_ROLE_KEY = "eyJhbGciOi..."
ADMIN_PASSWORD = "你自己設的強密碼"
```

帳號固定 **`admin`**，密碼只寫在 `ADMIN_PASSWORD`（不要 commit 進 repo）。

6. **Manage app → Reboot**（或 **Redeploy**／再按一次 Deploy）讓 Secrets 生效。

完成後用 **`https://xxx.streamlit.app`** 開啟；需 **admin + 密碼** 才能進入上傳／看報表。

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
