# 楽天RPPレポート自動取得 デプロイ手順

## 前提
- Google Cloud CLIインストール済み (`gcloud`)
- GCPプロジェクトが作成済み

## 1. GCPプロジェクト設定

```bash
# プロジェクト設定
gcloud config set project YOUR_PROJECT_ID

# 必要なAPIを有効化
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable cloudscheduler.googleapis.com
gcloud services enable secretmanager.googleapis.com
gcloud services enable sheets.googleapis.com
```

## 2. Secret Managerに認証情報を保存

```bash
# RMSログインID
echo -n "taste0504" | gcloud secrets create RMS_LOGIN_ID --data-file=-

# RMSパスワード
echo -n "Taste0120K" | gcloud secrets create RMS_PASSWORD --data-file=-
```

## 3. サービスアカウントに権限付与

```bash
# プロジェクト番号を取得
PROJECT_NUMBER=$(gcloud projects describe $(gcloud config get-value project) --format="value(projectNumber)")

# Cloud FunctionsのサービスアカウントにSecret Managerへのアクセスを付与
gcloud secrets add-iam-policy-binding RMS_LOGIN_ID \
  --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

gcloud secrets add-iam-policy-binding RMS_PASSWORD \
  --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

## 4. スプレッドシートの共有設定

Cloud Functionsのサービスアカウントにスプレッドシートの編集権限を付与:

```bash
# サービスアカウントのメールアドレスを確認
echo "${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
```

このメールアドレスをスプレッドシートの「共有」から編集者として追加する。

スプレッドシートURL: https://docs.google.com/spreadsheets/d/1V-CgRs9xpjbbaqb3OasgiCEYxfXP7_bpNsZFso-eiZI

## 5. Cloud Functionをデプロイ

```bash
cd ~/Downloads/rakuten-rpp-reporter

gcloud functions deploy fetchRppReport \
  --runtime=nodejs18 \
  --trigger-http \
  --allow-unauthenticated \
  --memory=1024MB \
  --timeout=120s \
  --region=asia-northeast1 \
  --set-secrets="RMS_LOGIN_ID=RMS_LOGIN_ID:latest,RMS_PASSWORD=RMS_PASSWORD:latest"
```

## 6. Cloud Schedulerで定期実行を設定

```bash
# 毎日朝9時に実行（日本時間）
gcloud scheduler jobs create http rakuten-rpp-daily \
  --schedule="0 9 * * *" \
  --uri="https://asia-northeast1-YOUR_PROJECT_ID.cloudfunctions.net/fetchRppReport" \
  --http-method=GET \
  --time-zone="Asia/Tokyo" \
  --location=asia-northeast1
```

## 7. 手動テスト

```bash
# Cloud Functionを手動で呼び出し
gcloud functions call fetchRppReport --region=asia-northeast1
```

## 注意事項

- **RMSのデータ変更・削除は一切行いません**（読み取り専用の操作のみ）
- RMSのUI変更があった場合、セレクタの修正が必要になる可能性があります
- Puppeteerのメモリ使用量が大きいため、Cloud Functionのメモリは1024MB以上を推奨
- 2段階認証が有効な場合、追加の対応が必要です
