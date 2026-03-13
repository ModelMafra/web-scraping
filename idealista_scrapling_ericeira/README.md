# idealista_scrapling_ericeira

Projeto base para testar a biblioteca `scrapling` na recolha de anuncios da Idealista focados na Ericeira, com retoma segura via `jsonl`.

## O que ja ficou pensado

- O identificador unico principal e o numero do URL do anuncio, por exemplo `https://www.idealista.pt/imovel/33078708/` tem `listing_id = 33078708`.
- A recolha foi separada em duas fases: descoberta de links e extração de detalhe.
- O estado de execução e append-only: `state/journal.jsonl`. Se o processo cair a meio, basta voltar a correr e ele salta o que ja foi concluido.
- As paginas HTML podem ser guardadas em `data/html/` para reprocessar depois sem voltar ao site.
- A Idealista devolveu uma pagina de proteção anti-bot num pedido HTTP simples, por isso o modo default ficou em `stealth`.
- No teste real de `2026-03-13`, ate `StealthySession` com browser instalado continuou a receber `403` e HTML de `captcha-delivery`, portanto o proximo passo mais provavel e usar proxy residencial ou browser remoto.
- O banner de cookies atual e gerido por Didomi, e o projeto tenta aceitá-lo automaticamente em modo browser.
- Existe agora uma UI local 16:9, mais simples, com logs visiveis na propria pagina e botoes rapidos para `Sacar 1 pagina`, `Sacar tudo` e `Continuar pendentes`.

## Estrutura

```text
idealista_scrapling_ericeira/
├── config/
│   ├── targets.toml
│   └── extract_fields.json
├── data/
│   ├── details/
│   │   └── ericeira_ads.jsonl
│   ├── discovery/
│   │   └── ericeira_listing_index.jsonl
│   └── html/
├── logs/
├── src/
│   └── idealista_ericeira_scraper/
│       ├── cli.py
│       ├── core.py
│       ├── parsers.py
│       ├── scraper.py
│       └── ui.py
├── state/
│   └── journal.jsonl
└── tests/
```

## Como instalar

```bash
cd /home/pedro/Projetos/Web_Scraping/idealista_scrapling_ericeira
source /home/pedro/Projetos/Web_Scraping/scrape_venv/bin/activate
pip install -e .
scrapling install
```

Se quiseres insistir no modo browser real, a documentação do Scrapling indica tambem `playwright install chrome` quando queres usar `real_chrome=True`.

## Proxies rotativos

O projeto ja aceita rotação nativa de proxies do próprio Scrapling.

1. Copia `config/proxies.example.txt` para `config/proxies.txt`.
2. Mete uma proxy por linha.
3. Define `proxies_file = "config/proxies.txt"` em `config/targets.toml`.

Notas:

- Se o ficheiro tiver 1 proxy valida, essa proxy e usada como proxy unica.
- Se tiver 2 ou mais, o projeto cria um `ProxyRotator` automaticamente.
- Podes usar linhas simples `http://user:pass@host:port` ou JSON no formato do Playwright.
- Para a Idealista, o que normalmente faz mais diferença e proxy residencial, nao datacenter.

## Comandos

Ver estado:

```bash
PYTHONPATH=src /home/pedro/Projetos/Web_Scraping/scrape_venv/bin/python -m idealista_ericeira_scraper status
```

Teste rapido de acesso com a sessao atual:

```bash
PYTHONPATH=src /home/pedro/Projetos/Web_Scraping/scrape_venv/bin/python -m idealista_ericeira_scraper warmup --mode stealth
```

Teste manual com browser visivel e perfil persistente:

```bash
PYTHONPATH=src /home/pedro/Projetos/Web_Scraping/scrape_venv/bin/python -m idealista_ericeira_scraper warmup --mode stealth --headful --manual
```

Fluxo mais simples:

1. Correr uma pagina inteira e gravar anuncio a anuncio logo no ficheiro final:

```bash
PYTHONPATH=src /home/pedro/Projetos/Web_Scraping/scrape_venv/bin/python -m idealista_ericeira_scraper page --max-pages 1
```

2. Ver os prints no terminal para acompanhar o que esta a ser aberto, saltado e guardado.
3. Se quiseres continuar, aumentar `--max-pages` ou voltar a correr o mesmo comando.
4. Se quiseres varrer tudo de seguida, usa:

```bash
PYTHONPATH=src /home/pedro/Projetos/Web_Scraping/scrape_venv/bin/python -m idealista_ericeira_scraper page --all-pages
```

Fluxo recomendado para testar a capacidade real de scraping do Scrapling:

1. Correr `warmup --mode stealth --headful --manual`.
2. O projeto tenta aceitar automaticamente o banner de cookies.
3. Se aparecer desafio, interagir manualmente no browser.
4. Carregar `Enter` no terminal quando a pagina estiver pronta.
5. Correr `discover` usando o mesmo `user_data_dir`.
6. Se o bloqueio desaparecer, avançar depois para `extract`.

Abrir a UI local para escolher os campos do detalhe e correr o scraper:

```bash
PYTHONPATH=src /home/pedro/Projetos/Web_Scraping/scrape_venv/bin/python -m idealista_ericeira_scraper ui
```

Isto abre um frontend em `http://127.0.0.1:8765/`, grava a selecao em `config/extract_fields.json`, mostra os logs da execucao na propria pagina e deixa correr:

- `Sacar 1 pagina`
- `Sacar tudo`
- `Continuar pendentes`

Descobrir anuncios:

```bash
PYTHONPATH=src /home/pedro/Projetos/Web_Scraping/scrape_venv/bin/python -m idealista_ericeira_scraper discover --max-pages 2
```

Sacar por pagina e gravar logo no output:

```bash
PYTHONPATH=src /home/pedro/Projetos/Web_Scraping/scrape_venv/bin/python -m idealista_ericeira_scraper page --max-pages 1
```

Sacar todas as paginas:

```bash
PYTHONPATH=src /home/pedro/Projetos/Web_Scraping/scrape_venv/bin/python -m idealista_ericeira_scraper page --all-pages
```

Extrair detalhe:

```bash
PYTHONPATH=src /home/pedro/Projetos/Web_Scraping/scrape_venv/bin/python -m idealista_ericeira_scraper extract --limit 10
```

Fazer tudo numa corrida:

```bash
PYTHONPATH=src /home/pedro/Projetos/Web_Scraping/scrape_venv/bin/python -m idealista_ericeira_scraper crawl --max-pages 2 --limit 10
```

Forçar um modo diferente:

```bash
PYTHONPATH=src /home/pedro/Projetos/Web_Scraping/scrape_venv/bin/python -m idealista_ericeira_scraper crawl --mode http
PYTHONPATH=src /home/pedro/Projetos/Web_Scraping/scrape_venv/bin/python -m idealista_ericeira_scraper crawl --mode dynamic
PYTHONPATH=src /home/pedro/Projetos/Web_Scraping/scrape_venv/bin/python -m idealista_ericeira_scraper crawl --mode stealth
```

## Ficheiros importantes

- `data/discovery/ericeira_listing_index.jsonl`: indice append-only com todos os links descobertos.
- `data/details/ericeira_ads.jsonl`: detalhe normalizado de cada anuncio.
- `config/extract_fields.json`: selecao de campos do detalhe guardada pela UI.
- `state/journal.jsonl`: journal append-only com eventos de `discover` e `extract`.
- `data/html/<listing_id>.html`: snapshot opcional da pagina para debug e reparse.
- `config/proxies.example.txt`: exemplo do formato para single proxy ou pool rotativa.

## Estrategia de retoma

1. O `discover` acrescenta linhas novas ao ficheiro de indice e regista eventos no journal.
2. O `extract` le o indice e ignora os `listing_id` ja presentes no output final.
3. Em caso de falha, o contador por anuncio fica no journal e respeita `max_retries`.
4. O estado e reconstruido a partir do proprio disco, sem ficheiros temporarios frágeis.

## Sugestoes importantes

- Comecar com `--max-pages 1 --limit 5` para validar seletores e bloqueios.
- Manter `save_html_snapshots = true` nas primeiras corridas.
- Se a Idealista apertar o anti-bot, usar `proxies_file` com proxies residenciais rotativos e, se preciso, combinar com `cdp_url`.
- Rever `robots.txt`, termos de uso e o ritmo de pedidos antes de correr em volume.
- Considerar um passo final que reparseie os HTML guardados para enriquecer campos sem voltar a tocar no site.
