#!/bin/sh
set -eu

MINIO_BUCKET="${MINIO_BUCKET:-waule-media}"
MINIO_WAULE_ACCESS_KEY="${MINIO_WAULE_ACCESS_KEY:-waule-newwaule}"

mc alias set home http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
mc mb --ignore-existing "home/$MINIO_BUCKET"

cat >/tmp/waule-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::$MINIO_BUCKET",
        "arn:aws:s3:::$MINIO_BUCKET/*"
      ]
    }
  ]
}
EOF

mc admin policy create home waule-media-rw /tmp/waule-policy.json 2>/dev/null || true
mc admin user add home "$MINIO_WAULE_ACCESS_KEY" "$MINIO_WAULE_SECRET_KEY" 2>/dev/null || true
mc admin policy attach home waule-media-rw --user "$MINIO_WAULE_ACCESS_KEY"
mc anonymous set none "home/$MINIO_BUCKET"

echo "Home MinIO bucket ready: $MINIO_BUCKET"
