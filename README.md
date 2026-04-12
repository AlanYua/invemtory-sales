# 庫存查驗 / 銷售統計（Streamlit）

對外網址由 **Streamlit Community Cloud** 提供；資料只存在 **Supabase**（需自行開專案）。

## 一、Supabase（約 5 分鐘）

1. 註冊 [supabase.com](https://supabase.com/) → **New project**。
2. 左側 **SQL Editor** → 貼上專案內 `supabase_schema.sql` → **Run**。
3. **Project Settings → API** 複製：
   - **Project URL**
   - **service_role** `secret`（給後端用，勿寫進前端／勿公開在 issue）

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
```

5. **Manage app → Reboot**（或再 deploy 一次）讓 Secrets 生效。

完成後 Cloud 會給你一個 `https://xxx.streamlit.app` 網址，外人即可上傳 Excel、看報表。

## 注意

- **免費 Cloud app 是公開的**：任何人知道網址都能進（沒登入保護）。若要控管存取需另加 auth 或改私有部署。
- 上傳單檔預設上限已調為約 200MB（見 `.streamlit/config.toml`）。

## 本機預覽（可選）

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
# 複製 secrets 到 .streamlit/secrets.toml 後：
streamlit run app.py
```
