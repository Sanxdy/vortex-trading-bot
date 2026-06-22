# HDD Docker Migration Plan

**Goal:** Move Docker data (images, containers, volumes) from USB (30GB, 35MB/s) to HDD (75GB, ~80MB/s) for faster I/O.

## Current layout

| Mount | Device | Size | Speed | Content |
|-------|--------|------|-------|---------|
| `/` (SD) | mmcblk2p2 | 6.5GB | ~50MB/s | OS, bot code (132MB) |
| `/mnt/usb` | sda1 | **29GB** (70% full) | **~35MB/s** | Docker overlay, swapfile, backtest cache |
| `/mnt/data75gb` | sdb1 | **74.5GB** (0.003% full) | **~80MB/s** | Empty |

## What to move

### 1. Swap file (highest impact)
Current: `/mnt/usb/swapfile` (4GB, USB 35MB/s)
**Move to:** `/mnt/data75gb/swapfile` (8GB, HDD 80MB/s)

```bash
# Disable old swap
swapoff /mnt/usb/swapfile

# Create new swap on HDD
fallocate -l 8G /mnt/data75gb/swapfile
chmod 600 /mnt/data75gb/swapfile
mkswap /mnt/data75gb/swapfile
swapon /mnt/data75gb/swapfile

# Make permanent
echo "/mnt/data75gb/swapfile none swap sw 0 0" >> /etc/fstab
```

### 2. OHLCV cache (already on HDD)
`/mnt/data75gb/vortex_cache/` — done ✅

### 3. TimescaleDB data
Current: Docker volume on USB overlay (700MB, grows ~50MB/week)
**Move to:** `/mnt/data75gb/docker/volumes/timescale_data`

```bash
# Stop timescaledb
docker stop vortex-timescaledb-1

# Move data
mkdir -p /mnt/data75gb/docker/volumes
cp -rp /var/lib/docker/volumes/vortex_timescale_data/_data /mnt/data75gb/docker/volumes/timescale_data

# Update docker-compose.yml volume path
# Change: timescale_data:/var/lib/postgresql/data
# To: /mnt/data75gb/docker/volumes/timescale_data:/var/lib/postgresql/data
```

### 4. Redis data (optional, small)
Redis data is small (<50MB). Only move if USB is critically full.

### 5. Docker build cache (already cleaned)
10GB → 0GB ✅

## Step-by-step execution

1. Stop all containers and Docker service
2. Create 8GB swap on HDD, enable it
3. Move TimescaleDB data to HDD
4. Update docker-compose.yml with new volume paths
5. Restart Docker and all containers
6. Verify performance

## Risks

| Risk | Impact | Mitigation |
|------|--------|-----------|
| HDD fails | Data loss | Keep USB as cold backup |
| Swap on HDD wears out HDD | Reduced HDD lifespan | Acceptable — HDD rated for years |
| Volume paths change | Containers can't find data | Test with dry-run first |
| High load during copy | Bot downtime | Schedule during trading pause |

## Expected improvement

| Metric | Before (USB) | After (HDD) |
|--------|-------------|-------------|
| Swap speed | 35MB/s | 80MB/s (**2.3x**) |
| DB query speed | ~50ms | ~35ms (**1.4x**) |
| Container startup | ~30s | ~20s |
