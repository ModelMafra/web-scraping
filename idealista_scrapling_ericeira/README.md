# idealista_scrapling_ericeira

Projeto para testar `scrapling` a fazer scraping de anúncios da Idealista na Ericeira, com gravação incremental em `jsonl` e retoma segura.

## Visão técnica do projeto

O projeto faz o trabalho em duas fases: primeiro descobre links de anúncios e depois entra no detalhe de cada anúncio para guardar o resultado final. Cada registo fica logo escrito em `data/details/ericeira_ads.jsonl`, por isso se algo falhar a meio é possível continuar sem perder o que já foi feito.

O identificador principal de cada anúncio é o `listing_id`, extraído do URL `/imovel/{id}/`. Esse ID é a base da retoma e evita guardar o mesmo anúncio repetidamente no output final.

Ficheiros principais:

- `config/targets.toml`: configuração do scraper
- `data/discovery/ericeira_listing_index.jsonl`: índice dos anúncios descobertos
- `data/details/ericeira_ads.jsonl`: detalhe final dos anúncios
- `state/journal.jsonl`: estado append-only da execução

O modo default é `stealth`, porque a Idealista tende a bloquear pedidos simples. O projeto tenta aceitar o banner de cookies automaticamente, mas em alguns casos pode ser preciso usar `warmup --mode stealth --headful --manual` para aquecer a sessão.

## Como usar

Instalar:

```bash
cd /home/pedro/Projetos/Web_Scraping/idealista_scrapling_ericeira
source /home/pedro/Projetos/Web_Scraping/scrape_venv/bin/activate
pip install -e .
scrapling install
```

Arrancar a UI:

```bash
cd /home/pedro/Projetos/Web_Scraping/idealista_scrapling_ericeira
/home/pedro/Projetos/Web_Scraping/scrape_venv/bin/idealista-ericeira ui
```

A app abre em `http://127.0.0.1:8765/`:

- `/` painel principal
- `/guia` versão do README dentro da app
- `/analise` análise dos dados já guardados

Dashboard analítico separado:

```bash
cd /home/pedro/Projetos/Web_Scraping/idealista_scrapling_ericeira
/home/pedro/Projetos/Web_Scraping/scrape_venv/bin/idealista-ericeira dashboard
```

Abre em `http://127.0.0.1:8766/` e serve para explorar os anúncios já guardados com filtros, gráficos comparativos e contexto estatístico.

Comandos principais:

```bash
/home/pedro/Projetos/Web_Scraping/scrape_venv/bin/idealista-ericeira page --max-pages 1
/home/pedro/Projetos/Web_Scraping/scrape_venv/bin/idealista-ericeira page --all-pages
/home/pedro/Projetos/Web_Scraping/scrape_venv/bin/idealista-ericeira extract --limit 10
```

Se o site bloquear a sessão:

```bash
/home/pedro/Projetos/Web_Scraping/scrape_venv/bin/idealista-ericeira warmup --mode stealth --headful --manual
```
