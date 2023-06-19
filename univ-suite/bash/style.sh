declare -p sts_bash_style &> /dev/null && return || declare -r sts_bash_style

# For styles, the uppercase variant turns off the style.
# For colors, the uppercase variant is the bright color.
# Suffix colors with "B" for background color.

# `style` and `style reset` are equivalent.
# Can specify multiple style arguments per call.

style() {
	(( $# )) || {
		printf '\033[m'
		return
	}
	declare s
	while s+=${ansi[$1]} && shift
	do (( $# )) && s+=';' || break
	done
	printf "\033[${s}m"
}

declare -Ai ansi=(
	    [reset]=0
	     [bold]=1
	     [Bold]=22
	      [dim]=2
	      [Dim]=22
	   [italic]=3
	   [Italic]=23
	[underline]=4
	[Underline]=24
	   [invert]=7
	   [Invert]=27
	   [strike]=9
	   [Strike]=29
	 [overline]=53
	 [Overline]=55

	    [black]=30
	      [red]=31
	    [green]=32
	   [yellow]=33
	     [blue]=34
	  [magenta]=35
	     [cyan]=36
	    [white]=37
	  [default]=39

	    [Black]=90
	      [Red]=91
	    [Green]=92
	   [Yellow]=93
	     [Blue]=94
	  [Magenta]=95
	     [Cyan]=96
	    [White]=97

	   [blackB]=40
	     [redB]=41
	   [greenB]=42
	  [yellowB]=43
	    [blueB]=44
	 [magentaB]=45
	    [cyanB]=46
	   [whiteB]=47
	 [defaultB]=49

	   [BlackB]=100
	     [RedB]=101
	   [GreenB]=102
	  [YellowB]=103
	    [BlueB]=104
	 [MagentaB]=105
	    [CyanB]=106
	   [WhiteB]=107
)
