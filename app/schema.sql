-- jobs: trạng thái từng job. status: queued -> processing -> done
CREATE TABLE IF NOT EXISTS jobs (
  job_id      text PRIMARY KEY,
  status      text NOT NULL DEFAULT 'queued',
  worker      text,
  lease_until timestamptz,                    -- hạn "thuê" job của worker đang xử lý
  attempts    int  NOT NULL DEFAULT 0,        -- số lần được worker nhận xử lý
  image_bytes int,
  created_at  timestamptz NOT NULL DEFAULT now(),
  started_at  timestamptz,
  finished_at timestamptz
);

-- results: kết quả suy luận. PRIMARY KEY (job_id) bảo đảm mỗi job chỉ có 1 kết quả
CREATE TABLE IF NOT EXISTS results (
  job_id      text PRIMARY KEY,
  worker      text,
  label       text,
  finished_at timestamptz NOT NULL DEFAULT now()
);

-- inference_runs: ghi mỗi lần "suy luận" thực sự chạy, dùng để đếm việc bị làm trùng
CREATE TABLE IF NOT EXISTS inference_runs (
  id         bigserial PRIMARY KEY,
  job_id     text NOT NULL,
  worker     text,
  started_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS inference_runs_job ON inference_runs (job_id);

-- outbox: sự kiện chờ phát sang RabbitMQ (chỉ dùng khi PUBLISH_MODE=outbox)
CREATE TABLE IF NOT EXISTS outbox (
  id           bigserial PRIMARY KEY,
  job_id       text NOT NULL,
  payload      jsonb NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox (id) WHERE published_at IS NULL;
