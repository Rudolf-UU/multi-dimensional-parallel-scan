set title "In-place prefix-sum (n = 67,108,864)"
set terminal pdf size 3.2,2.8
set output "./results-intel-12900/In-place_prefix-sum_n___67,108,864.pdf"
set key on
set key bottom right Right
set xrange [1:16]
set xtics (1, 4, 8, 12, 16, 20, 24, 28, 32)
set xlabel "Number of threads"
set yrange [0:3]
set ylabel "Speedup"
plot './results-intel-12900/In-place_prefix-sum_n___67,108,864.dat' using 1:2 title "Adaptive chained" ls 7 lw 1 pointsize 0.6 pointtype 13 with linespoints, \
  './results-intel-12900/In-place_prefix-sum_n___67,108,864.dat' using 1:3 title "Assisting column-wise chained" ls 6 lw 1 pointsize 0.7 with linespoints
