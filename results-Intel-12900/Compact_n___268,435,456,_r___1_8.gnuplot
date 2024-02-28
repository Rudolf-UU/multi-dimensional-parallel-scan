set title "Compact (n = 268,435,456, r = 1/8)"
set terminal pdf size 3.2,2.8
set output "./results/Compact_n___268,435,456,_r___1_8.pdf"
set key off
set xrange [1:24]
set xtics (1, 4, 8, 12, 16, 20, 24, 28, 32)
set xlabel "Number of threads"
set yrange [0:6]
set ylabel "Speedup"
plot './results/Compact_n___268,435,456,_r___1_8.dat' using 1:2 title "Scan-then-propagate" ls 3 lw 1 pointsize 0.6 pointtype 13 with linespoints, \
  './results/Compact_n___268,435,456,_r___1_8.dat' using 1:3 title "Reduce-then-scan" ls 5 lw 1 pointsize 0.6 with linespoints, \
  './results/Compact_n___268,435,456,_r___1_8.dat' using 1:4 title "Chained scan" ls 7 lw 1 pointsize 0.6 with linespoints, \
  './results/Compact_n___268,435,456,_r___1_8.dat' using 1:5 title "Assisted scan-t.-prop." ls 2 lw 1 pointsize 0.7 pointtype 12 with linespoints, \
  './results/Compact_n___268,435,456,_r___1_8.dat' using 1:6 title "Assisted reduce-t.-scan" ls 4 lw 1 pointsize 0.7 with linespoints, \
  './results/Compact_n___268,435,456,_r___1_8.dat' using 1:7 title "Adaptive chained scan" ls 6 lw 1 pointsize 0.7 with linespoints, \
  './results/Compact_n___268,435,456,_r___1_8.dat' using 1:8 title "oneTBB" ls 1 lw 1 pointsize 0.6 with linespoints, \
  './results/Compact_n___268,435,456,_r___1_8.dat' using 1:9 title "ParlayLib" ls 8 lw 1 pointsize 0.6 with linespoints
