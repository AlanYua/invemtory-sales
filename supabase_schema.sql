-- 在 Supabase Dashboard → SQL Editor 貼上執行一次即可。
-- Streamlit 端請用「Service role key」（只放在 Streamlit Cloud Secrets，勿寫進程式碼／勿 commit）。

create table if not exists public.app_state (
  id text primary key,
  payload text not null,
  updated_at timestamptz not null default now()
);

-- 若你堅持只用 anon key，需自行加 policy；建議 Streamlit 用 service role（後端請求，不進瀏覽器）。
