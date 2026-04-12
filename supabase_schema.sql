-- 在 Supabase Dashboard → SQL Editor 貼上執行一次即可。
-- 執行後若顯示「Success. No rows returned」= 建表成功（DDL 本來就不回傳列）。
-- Streamlit 端請用「Service role key」（只放在 Streamlit Cloud Secrets，勿寫進程式碼／勿 commit）。

create table if not exists public.app_state (
  id text primary key,
  payload text not null,
  updated_at timestamptz not null default now()
);

-- 若你堅持只用 anon key，需自行加 policy；建議 Streamlit 用 service role（後端請求，不進瀏覽器）。
