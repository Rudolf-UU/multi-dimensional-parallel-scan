set title "Prefix-sum (sh = [100, 100, 10000])"
set terminal pdf size 3.2,2.8
set output "./results-intel-12900/Prefix-sum_sh___[100,_100,_10000].pdf"
set key off
set xrange [1:16]
set xtics (1, 4, 8, 12, 16, 20, 24, 28, 32)
set xlabel "Number of threads"
set yrange [0:3]
set ylabel "Speedup"
plot './results-intel-12900/Prefix-sum_sh___[100,_100,_10000].dat' using 1:2 title "Sequential row-based" ls 5 lw 1 pointsize 0.6 with linespoints, \
  './results-intel-12900/Prefix-sum_sh___[100,_100,_10000].dat' using 1:3 title "Column-wise chained" ls 7 lw 1 pointsize 0.7 with linespoints, \
  './results-intel-12900/Prefix-sum_sh___[100,_100,_10000].dat' using 1:4 title "Row-wise chained" ls 8 lw 1 pointsize 0.7 with linespoints, \
  './results-intel-12900/Prefix-sum_sh___[100,_100,_10000].dat' using 1:5 title "Assisting column-wise chained" ls 6 lw 1 pointsize 0.7 with linespoints
