# Google Drive Upload Guide
**Document created:** 2025-11-14

## Setup rclone for Google Drive (One-time setup)

Since you're on an EC2 instance without a browser, you'll need to use remote authentication.

### Step 1: Configure rclone

Run this command:
```bash
rclone config
```

Follow these prompts:
```
n) New remote
name> gdrive
Storage> drive (or the number for Google Drive)
client_id> <press Enter to skip>
client_secret> <press Enter to skip>
scope> 1 (Full access)
root_folder_id> <press Enter>
service_account_file> <press Enter>
Edit advanced config? n
Use auto config? n (IMPORTANT: say 'n' since you're on a remote machine)
```

### Step 2: Authenticate on your local machine

After saying 'n' to auto config, rclone will give you a command to run on a machine with a browser.

On your **local computer** (Mac/Windows/Linux with a browser), run:
```bash
rclone authorize "drive"
```

This will:
1. Open a browser
2. Ask you to sign in to adam.thomas@gmail.com
3. Grant permissions
4. Display a long token

### Step 3: Paste token back to EC2

Copy the entire token (it looks like `{"access_token":...}`) and paste it back into the EC2 terminal when prompted.

Complete the setup:
```
y) Yes this is OK
q) Quit config
```

## Upload Files to Google Drive

### Option 1: Upload all populated metadata files

```bash
# Upload entire directory
rclone copy ~/pmcoaXMLs/populated_metadata/ \
  gdrive:populated_metadata_parquet \
  --drive-shared-with-me \
  --progress
```

### Option 2: Upload to specific folder ID

Using your folder ID: `1kil_o2eTWGSEvyoK5wj4pH83SRFYfzTI`

```bash
# Create a tar.gz archive first (optional, for faster transfer)
cd ~/pmcoaXMLs
tar -czf populated_metadata.tar.gz populated_metadata/

# Upload archive
rclone copy populated_metadata.tar.gz \
  gdrive:,root_folder_id=1kil_o2eTWGSEvyoK5wj4pH83SRFYfzTI \
  --progress

# OR upload individual files
rclone copy ~/pmcoaXMLs/populated_metadata/ \
  gdrive:,root_folder_id=1kil_o2eTWGSEvyoK5wj4pH83SRFYfzTI \
  --progress
```

### Option 3: Upload with better organization

```bash
# Create dated subfolder
TODAY=$(date +%Y-%m-%d)
rclone copy ~/pmcoaXMLs/populated_metadata/ \
  "gdrive:populated_metadata_$TODAY" \
  --drive-shared-with-me \
  --progress
```

## Useful Commands

### Check what's uploaded
```bash
rclone ls gdrive:
```

### Verify upload
```bash
rclone check ~/pmcoaXMLs/populated_metadata/ gdrive:populated_metadata_parquet
```

### Upload with bandwidth limit (if needed)
```bash
rclone copy ~/pmcoaXMLs/populated_metadata/ \
  gdrive:populated_metadata_parquet \
  --progress \
  --bwlimit 10M
```

## What Gets Uploaded

**Directory**: `~/pmcoaXMLs/populated_metadata/`
- 25 parquet files
- Total size: 568 MB
- Files: oa_comm_PMC*.parquet and oa_noncomm_PMC*.parquet

## Estimated Upload Time

With typical EC2 bandwidth:
- 568 MB ≈ 5-10 minutes
- Individual file transfers allow resuming if interrupted

## Troubleshooting

### Token expired
```bash
rclone config reconnect gdrive:
```

### Check configuration
```bash
rclone config show
```

### Test connection
```bash
rclone lsd gdrive:
```
