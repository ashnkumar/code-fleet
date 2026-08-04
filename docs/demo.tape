# Recording script for docs/demo.gif.
#
# Two steps, both required to reproduce the shipped file:
#
#   1. vhs docs/demo.tape
#   2. the ffmpeg pass at the bottom of this file
#
# The raw recording is ~47s, and 1030 of its 1189 frames are identical to the
# one before — a fleet run is mostly waiting. Shipping that means a reader
# watches a still frame for most of the loop. Step 2 drops the duplicates and
# replays what is left at a steady 12fps, so the GIF shows the events rather
# than the elapsed time. Nothing is cut: every frame that differs is kept, in
# order. Only the waiting goes.
#
# This drives the real `codefleet demo`: three runners, five tasks, live Claude
# Agent SDK sessions against examples/demo-repo. The run is not staged — the
# veto in the recording is a real denied write, and the numbers on screen are
# that run's. The task *graph* is staged: examples/demo-tasks.yaml orders T4 to
# touch api.py first so the collision is reproducible. See its header.
#
# Needs ANTHROPIC_API_KEY in the environment. Costs roughly $0.12 on the
# default model.

Output docs/demo.gif

Set Shell "bash"
Set FontSize 13
Set Width 1400
Set Height 860
Set Padding 18
Set Theme "Builtin Dark"
Set PlaybackSpeed 1.6
Set TypingSpeed 55ms

Type "uv run codefleet demo"
Sleep 600ms
Enter

# The run itself: assignment, the cascade, the veto, the retry, the summary.
Sleep 75s

# Hold on the final verdict so it is readable in a loop.
Sleep 6s

# --- Step 2: drop the duplicate frames -------------------------------------
#
# mpdecimate finds frames identical to their predecessor; setpts replays the
# survivors at 12fps; tpad holds the final verdict so it is readable in a loop.
# The two-pass palette keeps the terminal colours clean at 128 colours.
#
#   ffmpeg -i docs/demo.gif -vf \
#     "mpdecimate=hi=512:lo=256:frac=0.2,setpts=N/12/TB,tpad=stop_mode=clone:stop_duration=2.5,fps=12,palettegen=stats_mode=diff:max_colors=128" \
#     -y /tmp/pal.png
#
#   ffmpeg -i docs/demo.gif -i /tmp/pal.png -filter_complex \
#     "[0:v]mpdecimate=hi=512:lo=256:frac=0.2,setpts=N/12/TB,tpad=stop_mode=clone:stop_duration=2.5,fps=12[x];[x][1:v]paletteuse=dither=bayer:bayer_scale=4:diff_mode=rectangle" \
#     -loop 0 -y docs/demo.gif
