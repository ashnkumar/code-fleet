# Recording script for docs/demo.gif — regenerate with `vhs docs/demo.tape`.
#
# This drives the real `codefleet demo`: three runners, five tasks, live Claude
# Agent SDK sessions against examples/demo-repo. Nothing here is staged. The
# veto that shows up in the recording is a real denied write.
#
# Needs ANTHROPIC_API_KEY in the environment. Costs roughly $0.25 on the
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
