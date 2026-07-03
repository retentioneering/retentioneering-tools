-- Widget states cloud storage for retentioneering library
-- Run in Supabase SQL Editor

CREATE TABLE IF NOT EXISTS widget_states (
  id          UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  user_id     UUID NOT NULL DEFAULT auth.uid() REFERENCES auth.users(id) ON DELETE CASCADE,
  object_name TEXT NOT NULL,
  widget_type TEXT NOT NULL,
  state       JSONB NOT NULL DEFAULT '{}',
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  UNIQUE (user_id, object_name)
);

-- Auto-update updated_at on upsert
CREATE OR REPLACE FUNCTION update_widget_states_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER widget_states_updated_at
  BEFORE UPDATE ON widget_states
  FOR EACH ROW EXECUTE FUNCTION update_widget_states_updated_at();

-- RLS: users can only read and write their own states
ALTER TABLE widget_states ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Users can read own widget states"
  ON widget_states FOR SELECT
  USING (auth.uid() = user_id);

CREATE POLICY "Users can insert own widget states"
  ON widget_states FOR INSERT
  WITH CHECK (auth.uid() = user_id);

CREATE POLICY "Users can update own widget states"
  ON widget_states FOR UPDATE
  USING (auth.uid() = user_id)
  WITH CHECK (auth.uid() = user_id);

CREATE POLICY "Users can delete own widget states"
  ON widget_states FOR DELETE
  USING (auth.uid() = user_id);

-- Index for fast lookup by user
CREATE INDEX IF NOT EXISTS widget_states_user_id_idx ON widget_states (user_id);
