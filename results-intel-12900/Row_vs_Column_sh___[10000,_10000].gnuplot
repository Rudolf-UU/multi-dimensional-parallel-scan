set title "Row vs Column (sh = [10000, 10000])"
set terminal pdf size 3.2,2.8
set output "./results-intel-12900/Row_vs_Column_sh___[10000,_10000].pdf"
set key on
set key bottom right Right
set xrange [1:16]
set xtics (1, 4, 8, 12, 16, 20, 24, 28, 32)
set xlabel "Number of threads"
set yrange [0:3]
set ylabel "Speedup"
plot './results-intel-12900/Row_vs_Column_sh___[10000,_10000].dat' using 1:2 title "Column-wise chained" ls 7 lw 1 pointsize 0.7 with linespoints, \
  './results-intel-12900/Row_vs_Column_sh___[10000,_10000].dat' using 1:3 title "Row-wise chained" ls 8 lw 1 pointsize 0.7 with linespoints
