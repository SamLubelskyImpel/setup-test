#!/usr/bin/env bash
UNIVERSAL_INTEGRATIONS=`pwd | grep -Po '(.*)(?=((/.*?){0})$)'`
source "$UNIVERSAL_INTEGRATIONS"/bash/lib.sh
echo "$UNIVERSAL_INTEGRATIONS"

config_nginx() {
	aptdep nginx nginx || return

	declare src=$UNIVERSAL_INTEGRATIONS/utils/nginx.conf
	declare dst=/etc/nginx/sites-available/universal_integrations.conf
	declare sym=/etc/nginx/sites-enabled/universal_integrations
	declare return=0 restart=0

	if [[ -f $dst ]]; then
		cmp --silent -- "$src" "$dst" || {
			echo "Files '$src' and '$dst' differ."
			sudo cp --force --interactive -- "$src" "$dst"
			cmp --silent -- "$src" "$dst" && restart=1
		}
	elif [[ -a $dst ]]; then
		echo "'$dst' exists but is not a regular file"
		return=1
	else
		sudo cp -- "$src" "$dst" && restart=1 || return=$?
	fi

	if [[ -h $sym ]]; then
		declare target=$(readlink -f -- "$sym")
		[[ $target == "$dst" ]] || {
			echo "'$sym' points to '$target', not '$dst'."
			sudo ln --force --interactive --symbolic -- "$dst" "$sym"
			[[ $(readlink -f -- "$sym") == "$dst" ]] && restart=1
		}
	elif [[ -a $sym ]]; then
		echo "'$sym' exists but is not a symbolic link"
		return=1
	else
		sudo ln --symbolic -- "$dst" "$sym" && restart=1 || return=$?
	fi
	
	(( restart )) && {
		sudo systemctl restart nginx || return
		echo "Restarted nginx service"
	}

	return $return
}

return 0 2> /dev/null

config_nginx
