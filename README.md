# italia
Collezione di tutti gli enti territoriali italiani, ottenuti da wikipedia.
Disponibili in formato json e csv.

## Miglioramenti da effettuare
- Alcuni link di wikipedia sono rotti e risultano in pagine sbagliate.
- E necessaria pulizia ulteriore dei dati.

## Requisiti
- `uv`: fortemente consigliato per gestire i pacchetti di python semplicemente.
- `tor`: per evitare rate limiting da wikipedia.
- `mongodb`: non necessario, per dumpare i dati e fare query in modo semplice.

## Runtime
Per fare girare i vari script con `uv` usa questa sintassi.
`uv run python -m italia`
`uv run python -m dump`
`uv run python -m retrieve`

## Release command
`tar -czvf italia.tar.gz data/`