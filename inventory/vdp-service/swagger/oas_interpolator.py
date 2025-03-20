"""OAS spec contains data we do not want in client copy but need for deployment.
This script interpolates that data with the client copy for use with deployment.
Regular interpolation doesn't account for multi-line indentation needed for yaml files,
so this script is needed."""

from api_interpolation_data import VDP_API_INTERPOLATION_DATA


def interpolate_yaml_file(file_path: str, interpolation_dict: dict):
    """Interpolate OAS file for use in deployment."""
    place_holder_names = interpolation_dict.keys()
    interpolated_file_path = f'{file_path.split(".yaml")[0]}-interpolated.yaml'
    final_text = ""
    with open(file_path, "r") as source_file:
        lines = source_file.readlines()
        for line in lines:
            include_line = True
            for place_holder_name in place_holder_names:
                place_holder = "#{{" + place_holder_name + "}}"
                if place_holder in line:
                    include_line = False
                    indentation = line.split(place_holder)[0]
                    interpolation_lines = interpolation_dict[
                        place_holder_name
                    ].splitlines()
                    for interpolation_line in interpolation_lines:
                        final_text += f"{indentation}{interpolation_line}\n"
            if include_line:
                final_text += line

    with open(interpolated_file_path, "w+") as dest_file:
        dest_file.write(final_text)


interpolate_yaml_file(
    "swagger/vdp-api-oas.yaml",
    VDP_API_INTERPOLATION_DATA,
)
