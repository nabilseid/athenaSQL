DIR_ENV=".venv"
if [ ! -d "$DIR_ENV" ]; then
    echo "Creating vertual env: ${DIR_ENV}..."
    python3 -m venv "${DIR_ENV}"
    source "${DIR_ENV}/bin/activate"

    # install dependencies
    poetry install
else
    # activate venv
    . "${DIR_ENV}/bin/activate"
fi