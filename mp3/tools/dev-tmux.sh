tmux new-session -d "ssh ece428_1"
tmux split-window -h "ssh ece428_2"
tmux split-window -h "ssh ece428_3"
tmux split-window -h "ssh ece428_4"
tmux split-window -h "ssh ece428_5"
tmux select-layout even-horizontal

tmux selectp -t 4
tmux split-window -v "ssh ece428_10"
tmux selectp -t 3
tmux split-window -v "ssh ece428_9"
tmux selectp -t 2
tmux split-window -v "ssh ece428_8"
tmux selectp -t 1
tmux split-window -v "ssh ece428_7"
tmux selectp -t 0
tmux split-window -v "ssh ece428_6"

tmux -2 attach-session -d
