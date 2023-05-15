declare -p sts_bash_lib &> /dev/null && return || declare -r sts_bash_lib

error()       { declare return=$?; >&2 echo "$@"; return $return; }
declared()    { declare -p $1 &> /dev/null; }
cmdcheck()    { command -v "$1" > /dev/null; }
execheck()    [[ -x $(type -Pp "$1") ]]
cmddep()      { cmdcheck "$1" || { error "Dependency command '$1' not found"    ; return; }; }
exedep()      { execheck "$1" || { error "Dependency executable '$1' not found" ; return; }; }
aptdep()      { exedep "$1" || { shift; sudo apt-get --yes install "$@"; }; }
sudo-write()  { command sudo tee "$@" > /dev/null; }
sudo-append() { sudo-write --append "$@"; }
owneruser()   { stat --format='%U' "$1"; }
timestamp()   { date --utc +"%Y%m%dT%H%M%SZ"; }
download()    { wget --quiet --output-document=- --show-progress "$@"; }
publicIP()    { wget --quiet --output-document=- ifconfig.me; }
# FIXME this dig query has some issues with some region or some VPN
#publicIP()   { dig @resolver1.opendns.com -q myip.opendns.com "$@" +short; }
#publicIPv4() { publicIP -t A    -4; }
#publicIPv6() { publicIP -t AAAA -6; }
