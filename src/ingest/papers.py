"""Comprehensive arXiv paper harvesting via API.

Fetches papers from ALL arXiv categories using the arXiv API.
API docs: https://info.arxiv.org/help/api/index.html

Rate limit: 1 request per 3 seconds (we use 3s delay between requests)
"""
import xml.etree.ElementTree as ET
import time
from subsets_utils import get, save_raw_json, load_state, save_state

# Complete list of arXiv categories (as of 2024)
# Organized by archive
CATEGORIES = {
    # Computer Science (cs)
    "cs": [
        "cs.AI",   # Artificial Intelligence
        "cs.AR",   # Hardware Architecture
        "cs.CC",   # Computational Complexity
        "cs.CE",   # Computational Engineering
        "cs.CG",   # Computational Geometry
        "cs.CL",   # Computation and Language (NLP)
        "cs.CR",   # Cryptography and Security
        "cs.CV",   # Computer Vision
        "cs.CY",   # Computers and Society
        "cs.DB",   # Databases
        "cs.DC",   # Distributed Computing
        "cs.DL",   # Digital Libraries
        "cs.DM",   # Discrete Mathematics
        "cs.DS",   # Data Structures and Algorithms
        "cs.ET",   # Emerging Technologies
        "cs.FL",   # Formal Languages
        "cs.GL",   # General Literature
        "cs.GR",   # Graphics
        "cs.GT",   # Game Theory
        "cs.HC",   # Human-Computer Interaction
        "cs.IR",   # Information Retrieval
        "cs.IT",   # Information Theory
        "cs.LG",   # Machine Learning
        "cs.LO",   # Logic in Computer Science
        "cs.MA",   # Multiagent Systems
        "cs.MM",   # Multimedia
        "cs.MS",   # Mathematical Software
        "cs.NA",   # Numerical Analysis
        "cs.NE",   # Neural and Evolutionary Computing
        "cs.NI",   # Networking
        "cs.OH",   # Other CS
        "cs.OS",   # Operating Systems
        "cs.PF",   # Performance
        "cs.PL",   # Programming Languages
        "cs.RO",   # Robotics
        "cs.SC",   # Symbolic Computation
        "cs.SD",   # Sound
        "cs.SE",   # Software Engineering
        "cs.SI",   # Social and Information Networks
        "cs.SY",   # Systems and Control
    ],
    # Economics (econ)
    "econ": [
        "econ.EM",  # Econometrics
        "econ.GN",  # General Economics
        "econ.TH",  # Theoretical Economics
    ],
    # Electrical Engineering and Systems Science (eess)
    "eess": [
        "eess.AS",  # Audio and Speech Processing
        "eess.IV",  # Image and Video Processing
        "eess.SP",  # Signal Processing
        "eess.SY",  # Systems and Control
    ],
    # Mathematics (math)
    "math": [
        "math.AC",  # Commutative Algebra
        "math.AG",  # Algebraic Geometry
        "math.AP",  # Analysis of PDEs
        "math.AT",  # Algebraic Topology
        "math.CA",  # Classical Analysis
        "math.CO",  # Combinatorics
        "math.CT",  # Category Theory
        "math.CV",  # Complex Variables
        "math.DG",  # Differential Geometry
        "math.DS",  # Dynamical Systems
        "math.FA",  # Functional Analysis
        "math.GM",  # General Mathematics
        "math.GN",  # General Topology
        "math.GR",  # Group Theory
        "math.GT",  # Geometric Topology
        "math.HO",  # History and Overview
        "math.IT",  # Information Theory
        "math.KT",  # K-Theory
        "math.LO",  # Logic
        "math.MG",  # Metric Geometry
        "math.MP",  # Mathematical Physics
        "math.NA",  # Numerical Analysis
        "math.NT",  # Number Theory
        "math.OA",  # Operator Algebras
        "math.OC",  # Optimization and Control
        "math.PR",  # Probability
        "math.QA",  # Quantum Algebra
        "math.RA",  # Rings and Algebras
        "math.RT",  # Representation Theory
        "math.SG",  # Symplectic Geometry
        "math.SP",  # Spectral Theory
        "math.ST",  # Statistics Theory
    ],
    # Physics
    "physics": [
        # Astrophysics
        "astro-ph.CO",  # Cosmology
        "astro-ph.EP",  # Earth and Planetary
        "astro-ph.GA",  # Galaxies
        "astro-ph.HE",  # High Energy Astrophysics
        "astro-ph.IM",  # Instrumentation
        "astro-ph.SR",  # Solar and Stellar
        # Condensed Matter
        "cond-mat.dis-nn",   # Disordered Systems
        "cond-mat.mes-hall", # Mesoscale
        "cond-mat.mtrl-sci", # Materials Science
        "cond-mat.other",    # Other
        "cond-mat.quant-gas",# Quantum Gases
        "cond-mat.soft",     # Soft Matter
        "cond-mat.stat-mech",# Statistical Mechanics
        "cond-mat.str-el",   # Strongly Correlated
        "cond-mat.supr-con", # Superconductivity
        # General Relativity
        "gr-qc",
        # High Energy Physics
        "hep-ex",  # Experiment
        "hep-lat", # Lattice
        "hep-ph",  # Phenomenology
        "hep-th",  # Theory
        # Mathematical Physics
        "math-ph",
        # Nonlinear Sciences
        "nlin.AO",  # Adaptation
        "nlin.CD",  # Chaotic Dynamics
        "nlin.CG",  # Cellular Automata
        "nlin.PS",  # Pattern Formation
        "nlin.SI",  # Exactly Solvable
        # Nuclear
        "nucl-ex",  # Nuclear Experiment
        "nucl-th",  # Nuclear Theory
        # Physics (general)
        "physics.acc-ph",    # Accelerator Physics
        "physics.ao-ph",     # Atmospheric
        "physics.app-ph",    # Applied Physics
        "physics.atm-clus",  # Atomic Clusters
        "physics.atom-ph",   # Atomic Physics
        "physics.bio-ph",    # Biological Physics
        "physics.chem-ph",   # Chemical Physics
        "physics.class-ph",  # Classical Physics
        "physics.comp-ph",   # Computational Physics
        "physics.data-an",   # Data Analysis
        "physics.ed-ph",     # Physics Education
        "physics.flu-dyn",   # Fluid Dynamics
        "physics.gen-ph",    # General Physics
        "physics.geo-ph",    # Geophysics
        "physics.hist-ph",   # History of Physics
        "physics.ins-det",   # Instrumentation
        "physics.med-ph",    # Medical Physics
        "physics.optics",    # Optics
        "physics.plasm-ph",  # Plasma Physics
        "physics.pop-ph",    # Popular Physics
        "physics.soc-ph",    # Physics and Society
        "physics.space-ph",  # Space Physics
        # Quantum Physics
        "quant-ph",
    ],
    # Quantitative Biology (q-bio)
    "q-bio": [
        "q-bio.BM",  # Biomolecules
        "q-bio.CB",  # Cell Behavior
        "q-bio.GN",  # Genomics
        "q-bio.MN",  # Molecular Networks
        "q-bio.NC",  # Neurons and Cognition
        "q-bio.OT",  # Other
        "q-bio.PE",  # Populations and Evolution
        "q-bio.QM",  # Quantitative Methods
        "q-bio.SC",  # Subcellular Processes
        "q-bio.TO",  # Tissues and Organs
    ],
    # Quantitative Finance (q-fin)
    "q-fin": [
        "q-fin.CP",  # Computational Finance
        "q-fin.EC",  # Economics
        "q-fin.GN",  # General Finance
        "q-fin.MF",  # Mathematical Finance
        "q-fin.PM",  # Portfolio Management
        "q-fin.PR",  # Pricing of Securities
        "q-fin.RM",  # Risk Management
        "q-fin.ST",  # Statistical Finance
        "q-fin.TR",  # Trading and Microstructure
    ],
    # Statistics (stat)
    "stat": [
        "stat.AP",  # Applications
        "stat.CO",  # Computation
        "stat.ME",  # Methodology
        "stat.ML",  # Machine Learning
        "stat.OT",  # Other
        "stat.TH",  # Theory
    ],
}

# Flatten to list
ALL_CATEGORIES = []
for archive, cats in CATEGORIES.items():
    ALL_CATEGORIES.extend(cats)

ARXIV_NS = {
    'atom': 'http://www.w3.org/2005/Atom',
    'arxiv': 'http://arxiv.org/schemas/atom'
}


def fetch_category_batch(category: str, start: int = 0, max_results: int = 2000) -> list[dict]:
    """Fetch a batch of papers from a category."""
    url = "http://export.arxiv.org/api/query"
    params = {
        "search_query": f"cat:{category}",
        "sortBy": "submittedDate",
        "sortOrder": "descending",
        "start": start,
        "max_results": max_results,
    }

    response = get(url, params=params, timeout=120)
    root = ET.fromstring(response.content)

    papers = []
    for entry in root.findall('atom:entry', ARXIV_NS):
        id_elem = entry.find('atom:id', ARXIV_NS)
        if id_elem is None or 'arxiv.org' not in (id_elem.text or ''):
            continue

        paper = {
            "id": id_elem.text,
            "title": None,
            "summary": None,
            "published": None,
            "updated": None,
            "authors": [],
            "categories": [],
            "primary_category": None,
            "pdf_url": None,
            "doi": None,
            "journal_ref": None,
            "comment": None,
        }

        title = entry.find('atom:title', ARXIV_NS)
        if title is not None and title.text:
            paper["title"] = ' '.join(title.text.split())

        summary = entry.find('atom:summary', ARXIV_NS)
        if summary is not None and summary.text:
            paper["summary"] = ' '.join(summary.text.split())

        published = entry.find('atom:published', ARXIV_NS)
        if published is not None:
            paper["published"] = published.text

        updated = entry.find('atom:updated', ARXIV_NS)
        if updated is not None:
            paper["updated"] = updated.text

        paper["authors"] = [
            author.find('atom:name', ARXIV_NS).text
            for author in entry.findall('atom:author', ARXIV_NS)
            if author.find('atom:name', ARXIV_NS) is not None
        ]

        paper["categories"] = [
            cat.get('term')
            for cat in entry.findall('atom:category', ARXIV_NS)
        ]

        primary = entry.find('arxiv:primary_category', ARXIV_NS)
        if primary is not None:
            paper["primary_category"] = primary.get('term')

        pdf_link = entry.find("atom:link[@title='pdf']", ARXIV_NS)
        if pdf_link is not None:
            paper["pdf_url"] = pdf_link.get('href')

        doi = entry.find('arxiv:doi', ARXIV_NS)
        if doi is not None:
            paper["doi"] = doi.text

        journal = entry.find('arxiv:journal_ref', ARXIV_NS)
        if journal is not None:
            paper["journal_ref"] = journal.text

        comment = entry.find('arxiv:comment', ARXIV_NS)
        if comment is not None:
            paper["comment"] = comment.text

        papers.append(paper)

    return papers


def run():
    """Fetch comprehensive papers from all arXiv categories."""
    print(f"Fetching papers from {len(ALL_CATEGORIES)} arXiv categories...")

    state = load_state("arxiv")
    completed = set(state.get("completed", []))

    pending = [c for c in ALL_CATEGORIES if c not in completed]

    if not pending:
        print("  All categories complete. Running incremental update...")
        # Reset to refetch everything for freshness
        pending = ALL_CATEGORIES
        completed = set()
        save_state("arxiv", {"completed": []})

    print(f"  {len(pending)} categories to fetch...")

    for i, category in enumerate(pending, 1):
        print(f"  [{i}/{len(pending)}] {category}...")

        all_papers = []
        start = 0
        batch_size = 2000
        max_per_category = 10000

        while start < max_per_category:
            papers = fetch_category_batch(category, start=start, max_results=batch_size)

            if not papers:
                break

            all_papers.extend(papers)
            print(f"      {len(all_papers):,} papers...")

            if len(papers) < batch_size:
                break

            start += batch_size
            time.sleep(3)  # Rate limit

        if all_papers:
            # Save per category for manageability
            safe_name = category.replace(".", "_").replace("-", "_")
            save_raw_json(all_papers, f"papers_{safe_name}", compress=True)
            print(f"      -> Saved {len(all_papers):,} papers")

        completed.add(category)
        save_state("arxiv", {"completed": list(completed)})

    print("  Done.")
