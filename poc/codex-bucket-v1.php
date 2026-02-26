<?php

declare(strict_types=1);

use App\Commands\Visit;

require __DIR__ . '/../vendor/autoload.php';

if ($argc < 3) {
    fwrite(STDERR, "Usage: php85 poc/codex-bucket-v1.php <input.csv> <output.json> [workers]\n");
    exit(1);
}

$inputPath = $argv[1];
$outputPath = $argv[2];
$workers = isset($argv[3]) ? max(1, (int) $argv[3]) : 10;

$start = microtime(true);
gc_disable();

$fileSize = filesize($inputPath);

$dateLookup = [];
$dateLabels = [];
$dateBytes = [];
$numDates = 0;

$day = mktime(0, 0, 0, 1, 1, 2020);
$stop = mktime(0, 0, 0, 12, 31, 2026);
while ($day <= $stop) {
    $full = date('Y-m-d', $day);
    $key = substr($full, 3);
    $dateLookup[$key] = $numDates;
    $dateLabels[$numDates] = $full;
    $dateBytes[$numDates] = pack('v', $numDates);
    $numDates++;
    $day += 86400;
}

$blogPrefix = 25;
$slugId = [];
$slugLabels = [];
$numSlugs = 0;

foreach (Visit::all() as $visit) {
    $slug = substr($visit->uri, $blogPrefix);
    if (!isset($slugId[$slug])) {
        $slugId[$slug] = $numSlugs;
        $slugLabels[$numSlugs] = $slug;
        $numSlugs++;
    }
}

$fh = fopen($inputPath, 'rb');
stream_set_read_buffer($fh, 0);
$sample = fread($fh, min(2_097_152, $fileSize));
fclose($fh);

$slugId = [];
$slugLabels = [];
$numSlugs = 0;

$sampleEnd = strrpos($sample, "\n");
$sp = 0;
while ($sp < $sampleEnd) {
    $nl = strpos($sample, "\n", $sp);
    if ($nl === false) {
        break;
    }

    $slug = substr($sample, $sp + $blogPrefix, $nl - $sp - $blogPrefix - 26);
    if (!isset($slugId[$slug])) {
        $slugId[$slug] = $numSlugs;
        $slugLabels[$numSlugs] = $slug;
        $numSlugs++;
    }

    $sp = $nl + 1;
}
unset($sample);

foreach (Visit::all() as $visit) {
    $slug = substr($visit->uri, $blogPrefix);
    if (!isset($slugId[$slug])) {
        $slugId[$slug] = $numSlugs;
        $slugLabels[$numSlugs] = $slug;
        $numSlugs++;
    }
}

$totalCells = $numSlugs * $numDates;

$bounds = [0];
$fh = fopen($inputPath, 'rb');
for ($i = 1; $i < $workers; $i++) {
    $target = intdiv($fileSize * $i, $workers);
    fseek($fh, $target);
    fgets($fh);
    $bounds[] = ftell($fh);
}
$bounds[] = $fileSize;
fclose($fh);

$numChunks = count($bounds) - 1;
$tmpDir = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
$pid = getmypid();
$files = [];
$children = [];

for ($w = 1; $w < $numChunks; $w++) {
    $files[$w] = $tmpDir . '/cb1_' . $pid . '_' . $w;
    $child = pcntl_fork();

    if ($child === 0) {
        $counts = crunchBuckets(
            $inputPath,
            $bounds[$w],
            $bounds[$w + 1],
            $slugId,
            $dateLookup,
            $dateBytes,
            $numSlugs,
            $numDates,
            $totalCells,
        );
        file_put_contents($files[$w], pack('V*', ...$counts));
        exit(0);
    }

    $children[] = $child;
}

$tally = crunchBuckets(
    $inputPath,
    $bounds[0],
    $bounds[1],
    $slugId,
    $dateLookup,
    $dateBytes,
    $numSlugs,
    $numDates,
    $totalCells,
);

foreach ($children as $child) {
    pcntl_waitpid($child, $status);
}

for ($w = 1; $w < $numChunks; $w++) {
    $raw = file_get_contents($files[$w]);
    unlink($files[$w]);

    $j = 0;
    foreach (unpack('V*', $raw) as $v) {
        $tally[$j++] += $v;
    }
}

$out = fopen($outputPath, 'wb');
$json = '{';
$needComma = false;

for ($s = 0; $s < $numSlugs; $s++) {
    $base = $s * $numDates;
    $dateBuf = '';
    $firstEntry = true;

    for ($d = 0; $d < $numDates; $d++) {
        $n = $tally[$base + $d];
        if ($n === 0) {
            continue;
        }

        if (!$firstEntry) {
            $dateBuf .= ",\n";
        }

        $dateBuf .= '        "' . $dateLabels[$d] . '": ' . $n;
        $firstEntry = false;
    }

    if ($firstEntry) {
        continue;
    }

    if ($needComma) {
        $json .= ',';
    }
    $needComma = true;

    $escaped = str_replace('/', '\\/', $slugLabels[$s]);
    $json .= "\n    \"\\/blog\\/" . $escaped . "\": {\n" . $dateBuf . "\n    }";

    if (strlen($json) > 262144) {
        fwrite($out, $json);
        $json = '';
    }
}

$json .= "\n}";
fwrite($out, $json);
fclose($out);

fwrite(STDERR, sprintf("codex-bucket-v1: %.6f s\n", microtime(true) - $start));

function crunchBuckets(
    string $path,
    int $from,
    int $until,
    array $slugId,
    array $dateLookup,
    array $dateBytes,
    int $numSlugs,
    int $numDates,
    int $cells,
): array {
    $fh = fopen($path, 'rb');
    stream_set_read_buffer($fh, 0);
    fseek($fh, $from);

    $buckets = array_fill(0, $numSlugs, '');

    $consumed = 0;
    $total = $until - $from;
    $bufSize = 8_388_608;
    $prefix = 25;
    $stride = 52;

    while ($consumed < $total) {
        $want = $total - $consumed;
        $raw = fread($fh, $want > $bufSize ? $bufSize : $want);
        if ($raw === false || $raw === '') {
            break;
        }

        $end = strrpos($raw, "\n");
        if ($end === false) {
            continue;
        }

        $tail = strlen($raw) - $end - 1;
        if ($tail > 0) {
            fseek($fh, -$tail, SEEK_CUR);
        }

        $consumed += $end + 1;

        $p = $prefix;
        $fence = $end - 320;

        while ($p < $fence) {
            $sep = strpos($raw, ',', $p);
            $buckets[$slugId[substr($raw, $p, $sep - $p)]] .= $dateBytes[$dateLookup[substr($raw, $sep + 4, 7)]];
            $p = $sep + $stride;

            $sep = strpos($raw, ',', $p);
            $buckets[$slugId[substr($raw, $p, $sep - $p)]] .= $dateBytes[$dateLookup[substr($raw, $sep + 4, 7)]];
            $p = $sep + $stride;

            $sep = strpos($raw, ',', $p);
            $buckets[$slugId[substr($raw, $p, $sep - $p)]] .= $dateBytes[$dateLookup[substr($raw, $sep + 4, 7)]];
            $p = $sep + $stride;

            $sep = strpos($raw, ',', $p);
            $buckets[$slugId[substr($raw, $p, $sep - $p)]] .= $dateBytes[$dateLookup[substr($raw, $sep + 4, 7)]];
            $p = $sep + $stride;
        }

        while ($p < $end) {
            $sep = strpos($raw, ',', $p);
            if ($sep === false) {
                break;
            }

            $buckets[$slugId[substr($raw, $p, $sep - $p)]] .= $dateBytes[$dateLookup[substr($raw, $sep + 4, 7)]];
            $p = $sep + $stride;
        }
    }

    fclose($fh);

    $counts = array_fill(0, $cells, 0);

    for ($s = 0; $s < $numSlugs; $s++) {
        $packed = $buckets[$s];
        if ($packed === '') {
            continue;
        }

        $base = $s * $numDates;
        foreach (unpack('v*', $packed) as $did) {
            $counts[$base + $did]++;
        }
    }

    return $counts;
}
