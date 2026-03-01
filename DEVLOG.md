# DEVLOG - QuickMine Lite

Journal de developpement du projet QuickMine Lite.
Mis a jour a chaque session de travail.

---

## 2026-03-01 - Session 3 : variant explorer, export, déploiement Cloud, corrections

### Travail effectué

1. **Variant Explorer style Cortado** : visualisation des variantes en chevrons colorés (HTML/CSS/JS) avec légende, info pourcentage/count, scroll, slider de nombre de variantes
2. **Export graphes** : boutons PNG pour tous les graphes, export .bpmn XML pour BPMN
3. **Déploiement Streamlit Cloud** : création repo GitHub, `.gitignore`, `packages.txt` (graphviz), déploiement sur https://quickminelite.streamlit.app/
4. **Fit-to-view par défaut** : les graphes de processus s'ajustent automatiquement à la vue au chargement (requestAnimationFrame retry loop)
5. **Fix encodage UTF-8 SVG** : correction de l'affichage des symboles start/end (▶/■) dans les graphes — `atob()` remplacé par `TextDecoder('utf-8')`
6. **Fix slider DFG** : remplacement du slider "Threshold" (0-1, sans effet) par "Path coverage %" (0-100%, sémantique correcte)
7. **Fix API pm4py Cloud** : migration des imports bas-niveau (`pm4py.algo.discovery.inductive.algorithm`) vers l'API haut-niveau stable (`pm4py.discover_bpmn_inductive`, etc.) — corrige BPMN/Petri/ProcessTree sur Streamlit Cloud
8. **Passage event log direct** : les fonctions render acceptent `analyzer.log` directement au lieu de reconvertir `analyzer.df`

### Problèmes rencontrés et solutions

| Problème | Solution |
|----------|----------|
| SVG viewport collapse (graphes vides, 0x0) | Parse dimensions SVG (pt→px), set explicit pixel dimensions |
| `fitToView()` échoue avant dimensions iframe | `requestAnimationFrame` retry loop (20 attempts), `visibility: hidden` jusqu'au fit |
| Symboles ▶/■ affichés comme "â" + carrés | `atob()` ne gère que Latin-1 ; remplacé par `TextDecoder('utf-8')` |
| Slider DFG Threshold sans effet visible | Sémantique inversée ; remplacé par "Path coverage %" 0-100% |
| BPMN/Petri/ProcessTree cassés sur Streamlit Cloud | API pm4py bas-niveau incompatible ; migré vers `pm4py.discover_*` haut-niveau |
| Double conversion DataFrame→EventLog inutile | Passage de `analyzer.log` direct aux fonctions render |
| Slider variants crash si < 5 variantes | Condition `if total_variants <= 5` pour afficher toutes les variantes sans slider |

---

## 2026-02-28 - Session 2 : améliorations UX filtres + graphes interactifs

### Travail effectué

1. **Suppression du sélecteur Level** (Event/Case) dans les filtres custom : remplacé par auto-détection basée sur `column_classification` stocké à l'import
2. **Affichage des métriques filtrées** : les compteurs Events/Cases/Activities dans la sidebar affichent les valeurs filtrées avec delta (écart vs original) quand un filtre est actif
3. **Correction du pourcentage filtré** : passage de `:.0f` à `:.1f` pour éviter l'affichage "0%" sur les petits filtres
4. **Ajout du pourcentage en nombre de cases** : le résumé de filtrage affiche maintenant events ET cases avec pourcentage
5. **Graphes de processus interactifs** : remplacement des images PNG statiques par du SVG interactif avec zoom/pan/fit (JavaScript pur)

### Problèmes rencontrés et solutions

| Problème | Solution |
|----------|----------|
| Sélecteur Level redondant avec la détection auto Event/Case | Supprimé le radio, auto-détection via `column_classification` |
| Compteurs sidebar toujours à la valeur totale après filtre | Ajout de branche `if is_filtered` affichant les métriques filtrées avec `delta` |
| Pourcentage affiché "0%" pour 0.1% des événements | Format `:.1f` au lieu de `:.0f` |
| Pas de pourcentage en nombre de cases | Ajout d'une 2e ligne dans le résumé avec cases filtrés/total + pourcentage |
| Graphes de processus non zoomables (PNG statique) | Rendu SVG + wrapper HTML/JS avec pan/zoom/fit via `st.components.v1.html()` |

---

## 2026-02-28 - Session 1 : refactoring complet + filtres avancés + détection colonnes

### Travail effectué

1. **Refactoring complet** de QuickMineAnalytics (PyQt6 desktop) vers QuickMineLite (Streamlit web)
2. **Système de filtrage avancé** : 16 opérateurs, filtres event/case, expander dédié dans la sidebar
3. **Sélection de champs** dans la matrice de corrélation (Synthesis page)
4. **Détection automatique** des colonnes Event vs Case à l'import
5. **launch.bat** pour Windows avec détection automatique de Python

### Problèmes rencontrés et solutions

| Problème | Solution |
|----------|----------|
| `streamlit` non reconnu dans le PATH Windows | Utilisation de `py -m streamlit` avec fallback `python -m streamlit` |
| `pip` non reconnu dans le PATH Windows | Utilisation de `%PYTHON% -m pip` avec détection automatique de l'exécutable |
| Matrice de corrélation sans sélection de champs | Ajout de `st.multiselect` pour choisir les colonnes dans 06_synthesis.py |
| Filtres limités (activité + date uniquement) | Réécriture complète de la sidebar avec 16 opérateurs, filtres custom event/case |
| Pas de détection Event vs Case pour les colonnes | Ajout de `classify_columns()` dans helpers.py, appelé à l'import |

---

## Vue d'ensemble du projet

### Objectif

QuickMine Lite est une application web de **process mining** qui permet d'importer, analyser et visualiser des event logs (journaux d'événements) au format CSV. Elle remplace l'application desktop QuickMineAnalytics (PyQt6) par une interface web plus légère et accessible.

### Stack technique

| Composant | Technologie | Rôle |
|-----------|-------------|------|
| Frontend | **Streamlit** >= 1.36.0 | Interface web multi-pages, widgets interactifs |
| Moteur analytique | **DuckDB** >= 1.0.0 | Requêtes SQL analytiques in-memory (remplace SQLite + pandas lourds) |
| Process mining | **PM4Py** >= 2.7.0 | Algorithmes de process mining (DFG, BPMN, Petri Nets) |
| Visualisation | **Plotly** >= 5.18.0 | Charts interactifs (bar, pie, heatmap, scatter) |
| Graphes | **Graphviz** >= 0.20.0 | Rendu des graphes de processus (DFG, BPMN) |
| ML | **scikit-learn** >= 1.3.0 | Prédictions (next activity, remaining time, outcome) |
| Données | **pandas** >= 2.0.0, **numpy** >= 1.24.0 | Manipulation de DataFrames |
| Export | **openpyxl** >= 3.1.0 | Export Excel |

### Origine

Refactoring de **QuickMineAnalytics** v1.0.0, application desktop portable Windows construite avec :
- PyQt6 (GUI)
- SQLite (persistance)
- pm4py, pandas, plotly, scikit-learn
- ~16,900 lignes de code Python, 24 modules
- Python 3.13.0 Embeddable (distribué avec l'app)

---

## Chronologie des développements

> **Note** : Aucun des deux projets (source et cible) n'est sous contrôle de version git.
> La chronologie ci-dessous est reconstituée à partir des sessions de développement.

### Phase 1 : Fondations (session 2026-02-28)

1. Création de la structure de dossiers (`core/`, `analysis/`, `viz/`, `pages/`)
2. Extraction de `Config` et `helpers` depuis `src/utils.py`
3. Création de `DuckDBManager` avec 20+ requêtes SQL analytiques
4. Adaptation de `EventLogLoader` pour `st.file_uploader`
5. Point d'entrée `app.py` avec `st.navigation()` et sidebar
6. Page d'import CSV avec mapping de colonnes

### Phase 2 : Filtres et pages données

7. Adaptation de `FilterManager` + `FilterStrategyFactory` (suppression PyQt6)
8. Copie de `EventLogSampler` depuis `src/sampling_utils.py`
9. Pages : Event Log viewer, Case Explorer, Dashboard

### Phase 3 : Graphes de processus et analyseurs

10. Copie et adaptation de tous les analyseurs (`statistical`, `dfg`, `performance`, `correlation`, `quality`)
11. Classe composite `ProcessAnalyzer` sans `CachedAnalyzer` mixin
12. `ChartBuilder` Plotly (thème fixe au lieu de `Theme.get_current_theme()`)
13. Rendu DFG/BPMN/Petri via pm4py + Graphviz
14. Page Process Graph avec tabs DFG/BPMN/POWL/Petri

### Phase 4 : Pages d'analyse

15. Ad-Hoc Analysis avec pivot tables
16. Synthesis (multi-analyse avec expanders)
17. Attribute Changes
18. Bottleneck Analysis (4 types)
19. Variant Analysis via DuckDB

### Phase 5 : ML et finitions

20. ML Engine (extraction de `src/ml_predictions.py`, suppression QThread)
21. Page ML Predictions (3 tabs : next activity, remaining time, outcome)
22. `launch.bat` pour Windows

### Phase 6 : Améliorations post-livraison (session 1)

23. Sélection de champs dans la matrice de corrélation (Synthesis)
24. Système de filtrage complet : 16 opérateurs, filtres event/case custom
25. Détection automatique Event/Case des colonnes à l'import

### Phase 7 : UX filtres et graphes interactifs (session 2)

26. Suppression du Level radio, auto-détection Event/Case dans les filtres
27. Métriques filtrées avec delta dans la sidebar
28. Correction pourcentage filtré (events + cases)
29. Graphes de processus interactifs : SVG + JavaScript pan/zoom/fit

### Phase 8 : Variant explorer, export, déploiement Cloud (session 3)

30. Variant Explorer style Cortado (chevrons colorés HTML/CSS)
31. Export PNG graphes + export .bpmn XML
32. Déploiement Streamlit Cloud (GitHub + packages.txt)
33. Fix fit-to-view, encodage UTF-8, slider DFG, API pm4py Cloud
34. Migration API pm4py vers haut-niveau stable

---

## Décisions techniques et architecturales

### 1. Streamlit remplace PyQt6

**Justification** : Application accessible via navigateur, déploiement simplifié, pas de dépendance PyQt6 lourde. Streamlit offre un modèle déclaratif adapté au data analytics.

**Compromis** : Perte de la réactivité desktop (chaque interaction = rerun du script). Compensé par `st.session_state` pour la persistance et `@st.cache_data` pour le caching.

### 2. DuckDB remplace SQLite + pandas lourds

**Justification** : DuckDB est optimisé pour les requêtes analytiques (OLAP) sur des DataFrames pandas. Les opérations comme `groupby().agg()`, `shift(-1)`, `value_counts()` sont remplacées par des requêtes SQL avec fonctions de fenêtrage (`LEAD`, `LAG`, `ROW_NUMBER`, `STRING_AGG`).

**Implémentation** : `duckdb.register()` crée des vues zero-copy sur les DataFrames pandas (pas de copie mémoire). La base est in-memory, la source de vérité reste le CSV importé.

| Opération | Avant (pandas) | Après (DuckDB SQL) |
|-----------|----------------|---------------------|
| Stats globales | `df.groupby().agg()` | `SELECT COUNT(*), COUNT(DISTINCT ...)` |
| Arêtes DFG | `shift(-1)` par groupe | `LEAD(activity) OVER (PARTITION BY case_id)` |
| Variantes | `groupby().apply(join)` | `STRING_AGG(activity, ' -> ' ORDER BY ts)` |
| Durées des cas | `groupby().agg(min,max)` | `MAX(ts) - MIN(ts) GROUP BY case_id` |

### 3. Suppression du cache maison au profit de Streamlit

**Avant** : `AnalysisCache` + `CachedAnalyzer` mixin avec LRU eviction manuelle.
**Après** : `@st.cache_data` / `@st.cache_resource` de Streamlit + `st.session_state` pour les résultats d'analyse.

### 4. Strategy Pattern conservé pour les filtres

Les 16 stratégies de filtre (`EqualsStrategy`, `ContainsStrategy`, `RegexMatchStrategy`, etc.) et la `FilterStrategyFactory` ont été copiées telles quelles depuis `src/filter_strategies.py` (pure pandas, aucune dépendance PyQt6).

### 5. Architecture multipage Streamlit

Utilisation de l'API `st.navigation()` (Streamlit 1.36+) au lieu de l'ancien système de dossier `pages/`. Permet un contrôle explicite de la navigation et du regroupement des pages.

### 6. Détection Event/Case des colonnes

**Algorithme** : Un seul `df.groupby('case:concept:name')[custom_cols].nunique().max()` détermine si chaque colonne a au maximum 1 valeur unique par cas (→ Case) ou plus (→ Event). Exécuté une seule fois à l'import.

---

## Fonctionnalités développées

### Module `core/` - Couche données

| Module | Lignes | Description |
|--------|--------|-------------|
| `data_loader.py` | 558 | Import CSV, auto-détection colonnes, validation, conversion pm4py |
| `filter_engine.py` | 709 | 16 stratégies de filtre + FilterManager (event/case/time) |
| `sampling.py` | 731 | Échantillonnage stratifié/simple/systématique pour gros datasets |
| `duckdb_manager.py` | 385 | 20+ requêtes SQL analytiques (stats, DFG, variantes, bottlenecks) |
| `helpers.py` | 89 | `format_duration`, `format_number`, `ColumnNameMapper`, `classify_columns` |
| `config.py` | 37 | Constantes applicatives (limites, encodings, défauts) |

### Module `analysis/` - Moteurs d'analyse

| Module | Lignes | Description |
|--------|--------|-------------|
| `process_analyzer.py` | 177 | Classe composite héritant de tous les analyseurs |
| `statistical.py` | 130 | Statistiques résumées, distributions |
| `dfg_analyzer.py` | 178 | Découverte DFG, filtrage par fréquence/performance |
| `performance.py` | 65 | DFG avec métriques de durée |
| `correlation.py` | 89 | Corrélations numériques, durée par activité |
| `quality.py` | 216 | Score qualité, valeurs manquantes, anomalies temporelles |
| `bottleneck.py` | 366 | 4 types : activité, transition, ressource, cas |
| `ml_engine.py` | 322 | 3 modèles : next activity, remaining time, outcome (RandomForest) |

### Module `viz/` - Visualisations

| Module | Lignes | Description |
|--------|--------|-------------|
| `charts.py` | ~500 | ChartBuilder Plotly + Variant Explorer HTML (chevrons Cortado-style) |
| `process_maps.py` | ~420 | Rendu DFG/BPMN/Petri/ProcessTree via pm4py + Graphviz + SVG interactif + export |
| `gantt.py` | 213 | Diagramme de Gantt pour les cas |

### Pages Streamlit

| Page | Lignes | Description |
|------|--------|-------------|
| `data_import.py` | 182 | Upload CSV, mapping colonnes, config sampling, classification colonnes |
| `01_dashboard.py` | 95 | Métriques clés, events over time, activity distribution |
| `02_process_graph.py` | ~180 | DFG/BPMN/Petri/ProcessTree avec coverage slider + zoom/pan/fit + export PNG/BPMN |
| `03_case_explorer.py` | 69 | Liste des cas, détail par cas, Gantt |
| `04_event_log.py` | 70 | Dataframe paginé avec sélection de colonnes |
| `05_adhoc_analysis.py` | 195 | Analyse paramétrable + pivot tables |
| `06_synthesis.py` | 169 | Multi-analyse (stats, corrélations, qualité, variantes, activités) |
| `07_attribute_changes.py` | 135 | Transitions d'attributs entre événements |
| `08_bottleneck.py` | 151 | 4 tabs : activité, transition, ressource, cas |
| `09_variants.py` | ~160 | Variant Explorer Cortado-style + charts + détail + CSV export |
| `10_ml_predictions.py` | 166 | 3 modèles ML avec entraînement et évaluation |

### Sidebar (dans `app.py` - 439 lignes)

- Info dataset (fichier, events, cases, activities)
- **Filtres rapides** : multiselect activités, plage de dates
- **Filtres custom** (expander) : sélecteur colonne avec tag [Event]/[Case], 16 opérateurs, valeur adaptative
- Affichage des filtres actifs avec suppression individuelle
- Boutons Apply / Clear All
- Résumé du filtrage (events/cases filtrés, pourcentage)

---

## Comparaison avec l'application source

| Aspect | QuickMineAnalytics (PyQt6) | QuickMineLite (Streamlit) |
|--------|----------------------------|---------------------------|
| Lignes de code | ~16,900 | ~6,940 |
| Modules Python | 24 | 34 (plus modulaire) |
| Interface | Desktop (PyQt6 tabs) | Web (Streamlit multipage) |
| Base de données | SQLite (769 MB sur disque) | DuckDB in-memory (zero-copy) |
| Cache | LRU maison (`AnalysisCache`) | `st.session_state` + `@st.cache_data` |
| Threading | QThread pour tâches longues | `st.spinner` (synchrone) |
| Thème | Dark/Light avec JSON config | Thème Streamlit (config.toml) |
| Filtres | PyQt6 FilterPanel (780 lignes) | Sidebar expander (16 opérateurs) |
| Déploiement | Windows portable (Python embarqué) | `pip install` + `streamlit run` |
| Taille distribution | ~4.9 GB (avec Python + packages) | ~quelques MB (code seul) |

### Fichiers réutilisés directement

| Fichier source | Réutilisation | Adaptation |
|----------------|---------------|------------|
| `src/filter_strategies.py` | 100% | Pure pandas, copié tel quel |
| `src/bottleneck_analysis.py` | 100% | Pure pandas, copié tel quel |
| `src/sampling_utils.py` | 95% | Callback progress adapté |
| `src/process_analyzer/*.py` | 90-100% | Suppression cache checks manuels |
| `src/data_loader.py` | 95% | Ajout `load_from_uploaded_file()` |
| `src/filter_manager.py` | 80% | Suppression `QObject`, `pyqtSignal` |
| `src/visualizations.py` | 80% | Suppression `Theme`, `TempFileManager` |
| `src/ml_predictions.py` | 40% | Extraction logique ML, suppression QThread/UI |

---

## État actuel du projet

### Fonctionnel

- Import CSV avec auto-détection de colonnes et classification Event/Case
- 10 pages d'analyse fonctionnelles
- Système de filtrage complet (16 opérateurs, event/case, time range)
- Visualisations Plotly interactives
- DuckDB pour les agrégations analytiques
- Export CSV/Excel sur les pages pertinentes

### Points d'attention

- **Tests** : aucun test unitaire ou d'intégration n'a été écrit
- **launch.bat** : problèmes de PATH Python sur certaines machines Windows (résolu mais non re-testé)
- **Performance** : la détection Event/Case (`classify_columns`) fait un `groupby().nunique()` qui peut être lent sur des datasets > 1M lignes
- **DuckDB `register()`** : méthode dépréciée dans DuckDB récent, à migrer vers `conn.register()` ou `conn.sql("CREATE VIEW")`
- **pm4py compatibilité** : utiliser l'API haut-niveau (`pm4py.discover_*`) plutôt que les imports bas-niveau (`pm4py.algo.discovery.*`) pour la compatibilité Cloud

### Prochaines étapes envisagées

- [x] ~~Initialiser un dépôt git et faire un commit initial~~
- [x] ~~Déployer sur Streamlit Cloud~~ → https://quickminelite.streamlit.app/
- [ ] Écrire des tests unitaires (au minimum pour `filter_engine`, `duckdb_manager`, `classify_columns`)
- [ ] Tester avec des fichiers CSV réels de process mining
- [ ] Ajouter le support de formats additionnels (XES, .pmfast)
- [ ] Optimiser `classify_columns` via DuckDB SQL au lieu de pandas groupby
- [ ] Ajouter un système d'export PDF pour les rapports de synthèse

---

## Annexe : Structure du projet (arborescence commentée)

```
QuickMineLite/
├── .streamlit/
│   └── config.toml              # Config Streamlit (thème, upload 500MB, layout wide)
│
├── core/                         # COUCHE DONNÉES
│   ├── __init__.py
│   ├── config.py                 # Constantes : APP_NAME, VERSION, limites, encodings
│   ├── data_loader.py            # EventLogLoader : import CSV, détection colonnes, validation
│   ├── duckdb_manager.py         # DuckDBManager : 20+ requêtes SQL (stats, DFG, variantes...)
│   ├── filter_engine.py          # 16 FilterStrategy + FilterStrategyFactory + FilterManager
│   ├── helpers.py                # format_duration, format_number, ColumnNameMapper, classify_columns
│   └── sampling.py               # EventLogSampler : stratifié, simple, systématique
│
├── analysis/                     # MOTEURS D'ANALYSE
│   ├── __init__.py
│   ├── base_analyzer.py          # BaseAnalyzer : conversion pm4py, validation
│   ├── statistical.py            # Stats résumées, distributions
│   ├── dfg_analyzer.py           # Découverte DFG, filtrage
│   ├── performance.py            # DFG avec métriques de durée
│   ├── correlation.py            # Corrélations numériques, events-duration
│   ├── quality.py                # Score qualité, missing values, anomalies timestamps
│   ├── bottleneck.py             # 4 types de bottleneck (activité, transition, ressource, cas)
│   ├── ml_engine.py              # 3 modèles ML : next activity, remaining time, outcome
│   └── process_analyzer.py       # Classe composite (hérite de tous les analyseurs)
│
├── viz/                          # VISUALISATIONS
│   ├── __init__.py
│   ├── charts.py                 # ChartBuilder Plotly (bar, pie, heatmap, scatter, box, histogram)
│   ├── process_maps.py           # Rendu DFG/BPMN/Petri/ProcessTree (pm4py + Graphviz)
│   └── gantt.py                  # Diagramme de Gantt (Plotly)
│
├── pages/                        # PAGES STREAMLIT
│   ├── __init__.py
│   ├── data_import.py            # Import CSV + mapping + sampling + classification colonnes
│   ├── 01_dashboard.py           # Métriques, events over time, activity distribution
│   ├── 02_process_graph.py       # DFG/BPMN/POWL/Petri avec tabs et sliders
│   ├── 03_case_explorer.py       # Liste cas + détail + Gantt
│   ├── 04_event_log.py           # Dataframe paginé avec sélection colonnes
│   ├── 05_adhoc_analysis.py      # Analyse paramétrable + pivot tables
│   ├── 06_synthesis.py           # Multi-analyse (stats, corrélations, qualité, variantes)
│   ├── 07_attribute_changes.py   # Transitions d'attributs
│   ├── 08_bottleneck.py          # 4 tabs bottleneck + recommandations
│   ├── 09_variants.py            # Découverte et comparaison de variantes
│   └── 10_ml_predictions.py      # 3 modèles ML avec entraînement + évaluation
│
├── app.py                        # POINT D'ENTRÉE : navigation, sidebar, filtres, session_state
├── launch.bat                    # Lanceur Windows (détection Python, install deps, run)
├── requirements.txt              # Dépendances Python
├── packages.txt                  # Dépendances système Streamlit Cloud (graphviz)
├── .gitignore                    # Exclusions git (pycache, venv, etc.)
├── DEVLOG.md                     # Ce fichier
│
└── Déploiement :
    ├── GitHub : https://github.com/cequoideja/QuickMineLite.git
    └── Streamlit Cloud : https://quickminelite.streamlit.app/
```

### Statistiques du code

| Module | Fichiers | Lignes |
|--------|----------|--------|
| core/ | 7 | 2,509 |
| analysis/ | 10 | 1,638 |
| viz/ | 4 | 840 |
| pages/ | 12 | 1,512 |
| app.py | 1 | 439 |
| **Total** | **34** | **6,938** |
