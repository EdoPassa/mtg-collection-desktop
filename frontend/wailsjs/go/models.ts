export namespace analysis {
	
	export class DeckDrawAnalysisRequest {
	    deckId: number;
	    formatTarget: string;
	    sampleSize: number;
	    oracleId: string;
	    minCardCopies: number;
	    minLands: number;
	    landsInDeck: number;
	
	    static createFrom(source: any = {}) {
	        return new DeckDrawAnalysisRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.deckId = source["deckId"];
	        this.formatTarget = source["formatTarget"];
	        this.sampleSize = source["sampleSize"];
	        this.oracleId = source["oracleId"];
	        this.minCardCopies = source["minCardCopies"];
	        this.minLands = source["minLands"];
	        this.landsInDeck = source["landsInDeck"];
	    }
	}
	export class DeckDrawAnalysisResult {
	    populationN: number;
	    deckTotal: number;
	    targetSize: number;
	    detectedLands: number;
	    effectiveLandsK: number;
	    effectiveSampleSize: number;
	    cardProbability: number;
	    cardProbabilityFormatted: string;
	    landProbability: number;
	    landProbabilityFormatted: string;
	    sizeWarning?: string;
	
	    static createFrom(source: any = {}) {
	        return new DeckDrawAnalysisResult(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.populationN = source["populationN"];
	        this.deckTotal = source["deckTotal"];
	        this.targetSize = source["targetSize"];
	        this.detectedLands = source["detectedLands"];
	        this.effectiveLandsK = source["effectiveLandsK"];
	        this.effectiveSampleSize = source["effectiveSampleSize"];
	        this.cardProbability = source["cardProbability"];
	        this.cardProbabilityFormatted = source["cardProbabilityFormatted"];
	        this.landProbability = source["landProbability"];
	        this.landProbabilityFormatted = source["landProbabilityFormatted"];
	        this.sizeWarning = source["sizeWarning"];
	    }
	}
	export class DeckTagAnalysisRequest {
	    deckId: number;
	    formatTarget: string;
	    sampleSize: number;
	    minTagCards: number;
	    tagFocus: number;
	
	    static createFrom(source: any = {}) {
	        return new DeckTagAnalysisRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.deckId = source["deckId"];
	        this.formatTarget = source["formatTarget"];
	        this.sampleSize = source["sampleSize"];
	        this.minTagCards = source["minTagCards"];
	        this.tagFocus = source["tagFocus"];
	    }
	}
	export class TagDeckStat {
	    tagId: number;
	    name: string;
	    color?: string;
	    copiesInDeck: number;
	    copiesInLibrary?: number;
	    nextDrawProb?: number;
	    nextDrawProbFormatted?: string;
	    sampleProb: number;
	    sampleProbFormatted: string;
	
	    static createFrom(source: any = {}) {
	        return new TagDeckStat(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.tagId = source["tagId"];
	        this.name = source["name"];
	        this.color = source["color"];
	        this.copiesInDeck = source["copiesInDeck"];
	        this.copiesInLibrary = source["copiesInLibrary"];
	        this.nextDrawProb = source["nextDrawProb"];
	        this.nextDrawProbFormatted = source["nextDrawProbFormatted"];
	        this.sampleProb = source["sampleProb"];
	        this.sampleProbFormatted = source["sampleProbFormatted"];
	    }
	}
	export class DeckTagAnalysisResult {
	    populationN: number;
	    deckTotal: number;
	    tags: TagDeckStat[];
	    focus?: TagDeckStat;
	    sizeWarning?: string;
	
	    static createFrom(source: any = {}) {
	        return new DeckTagAnalysisResult(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.populationN = source["populationN"];
	        this.deckTotal = source["deckTotal"];
	        this.tags = this.convertValues(source["tags"], TagDeckStat);
	        this.focus = this.convertValues(source["focus"], TagDeckStat);
	        this.sizeWarning = source["sizeWarning"];
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class DrawStats {
	    landsInHand: number;
	    libraryRemaining: number;
	    nextDrawLandProb: number;
	    nextDrawLandProbFormatted: string;
	    nextDrawCardProb: number;
	    nextDrawCardProbFormatted: string;
	    oracleIdUsed?: string;
	    nextDrawTagProb: number;
	    nextDrawTagProbFormatted: string;
	    tagIdUsed?: number;
	    afterOneDrawLandsProb: number;
	    afterOneDrawLandsProbFormatted: string;
	    minLandsThreshold: number;
	    tags?: TagDeckStat[];
	
	    static createFrom(source: any = {}) {
	        return new DrawStats(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.landsInHand = source["landsInHand"];
	        this.libraryRemaining = source["libraryRemaining"];
	        this.nextDrawLandProb = source["nextDrawLandProb"];
	        this.nextDrawLandProbFormatted = source["nextDrawLandProbFormatted"];
	        this.nextDrawCardProb = source["nextDrawCardProb"];
	        this.nextDrawCardProbFormatted = source["nextDrawCardProbFormatted"];
	        this.oracleIdUsed = source["oracleIdUsed"];
	        this.nextDrawTagProb = source["nextDrawTagProb"];
	        this.nextDrawTagProbFormatted = source["nextDrawTagProbFormatted"];
	        this.tagIdUsed = source["tagIdUsed"];
	        this.afterOneDrawLandsProb = source["afterOneDrawLandsProb"];
	        this.afterOneDrawLandsProbFormatted = source["afterOneDrawLandsProbFormatted"];
	        this.minLandsThreshold = source["minLandsThreshold"];
	        this.tags = this.convertValues(source["tags"], TagDeckStat);
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class FormatTarget {
	    id: string;
	    label: string;
	    size: number;
	
	    static createFrom(source: any = {}) {
	        return new FormatTarget(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.id = source["id"];
	        this.label = source["label"];
	        this.size = source["size"];
	    }
	}
	export class HypergeometricRequest {
	    population: number;
	    successesInPopulation: number;
	    sampleSize: number;
	    minSuccessesInSample: number;
	    mode: string;
	
	    static createFrom(source: any = {}) {
	        return new HypergeometricRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.population = source["population"];
	        this.successesInPopulation = source["successesInPopulation"];
	        this.sampleSize = source["sampleSize"];
	        this.minSuccessesInSample = source["minSuccessesInSample"];
	        this.mode = source["mode"];
	    }
	}
	export class HypergeometricResult {
	    probability: number;
	    probabilityFormatted: string;
	
	    static createFrom(source: any = {}) {
	        return new HypergeometricResult(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.probability = source["probability"];
	        this.probabilityFormatted = source["probabilityFormatted"];
	    }
	}
	export class SimulationCard {
	    slotId: string;
	    oracleId: string;
	    name: string;
	    isLand: boolean;
	    typeLine?: string;
	    imageSmall?: string;
	    imageNormal?: string;
	    colorIdentity?: string[];
	
	    static createFrom(source: any = {}) {
	        return new SimulationCard(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.slotId = source["slotId"];
	        this.oracleId = source["oracleId"];
	        this.name = source["name"];
	        this.isLand = source["isLand"];
	        this.typeLine = source["typeLine"];
	        this.imageSmall = source["imageSmall"];
	        this.imageNormal = source["imageNormal"];
	        this.colorIdentity = source["colorIdentity"];
	    }
	}
	export class SimulationState {
	    sessionId: string;
	    phase: string;
	    hand: SimulationCard[];
	    libraryCount: number;
	    mulliganCount: number;
	    canMulligan: boolean;
	    canDraw: boolean;
	    stats: DrawStats;
	    deckId: number;
	    formatTarget: string;
	
	    static createFrom(source: any = {}) {
	        return new SimulationState(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.sessionId = source["sessionId"];
	        this.phase = source["phase"];
	        this.hand = this.convertValues(source["hand"], SimulationCard);
	        this.libraryCount = source["libraryCount"];
	        this.mulliganCount = source["mulliganCount"];
	        this.canMulligan = source["canMulligan"];
	        this.canDraw = source["canDraw"];
	        this.stats = this.convertValues(source["stats"], DrawStats);
	        this.deckId = source["deckId"];
	        this.formatTarget = source["formatTarget"];
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}

}

export namespace cards {
	
	export class CardIdentity {
	    oracleId: string;
	    name: string;
	    scryfallUri: string;
	    typeLine?: string;
	    manaCost?: string;
	    colorIdentity?: string[];
	    imageSmall?: string;
	    imageNormal?: string;
	
	    static createFrom(source: any = {}) {
	        return new CardIdentity(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.oracleId = source["oracleId"];
	        this.name = source["name"];
	        this.scryfallUri = source["scryfallUri"];
	        this.typeLine = source["typeLine"];
	        this.manaCost = source["manaCost"];
	        this.colorIdentity = source["colorIdentity"];
	        this.imageSmall = source["imageSmall"];
	        this.imageNormal = source["imageNormal"];
	    }
	}
	export class CollectionFolder {
	    id: number;
	    name: string;
	    parentId?: number;
	
	    static createFrom(source: any = {}) {
	        return new CollectionFolder(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.id = source["id"];
	        this.name = source["name"];
	        this.parentId = source["parentId"];
	    }
	}
	export class CollectionTag {
	    id: number;
	    name: string;
	    color?: string;
	    cardCount?: number;
	
	    static createFrom(source: any = {}) {
	        return new CollectionTag(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.id = source["id"];
	        this.name = source["name"];
	        this.color = source["color"];
	        this.cardCount = source["cardCount"];
	    }
	}
	export class CollectionItem {
	    card: CardIdentity;
	    quantity: number;
	    lentQty: number;
	    inDeck: boolean;
	    available: number;
	    allocatedQty?: number;
	    unassignedQty?: number;
	    tags?: CollectionTag[];
	
	    static createFrom(source: any = {}) {
	        return new CollectionItem(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.card = this.convertValues(source["card"], CardIdentity);
	        this.quantity = source["quantity"];
	        this.lentQty = source["lentQty"];
	        this.inDeck = source["inDeck"];
	        this.available = source["available"];
	        this.allocatedQty = source["allocatedQty"];
	        this.unassignedQty = source["unassignedQty"];
	        this.tags = this.convertValues(source["tags"], CollectionTag);
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	
	export class Deck {
	    id: number;
	    name: string;
	
	    static createFrom(source: any = {}) {
	        return new Deck(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.id = source["id"];
	        this.name = source["name"];
	    }
	}
	export class DeckCard {
	    card: CardIdentity;
	    quantity: number;
	    board?: string;
	
	    static createFrom(source: any = {}) {
	        return new DeckCard(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.card = this.convertValues(source["card"], CardIdentity);
	        this.quantity = source["quantity"];
	        this.board = source["board"];
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class FolderCard {
	    card: CardIdentity;
	    quantity: number;
	    lentQty?: number;
	    inDeck?: boolean;
	    available?: number;
	    tags?: CollectionTag[];
	
	    static createFrom(source: any = {}) {
	        return new FolderCard(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.card = this.convertValues(source["card"], CardIdentity);
	        this.quantity = source["quantity"];
	        this.lentQty = source["lentQty"];
	        this.inDeck = source["inDeck"];
	        this.available = source["available"];
	        this.tags = this.convertValues(source["tags"], CollectionTag);
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class LentCard {
	    id: number;
	    card: CardIdentity;
	    quantity: number;
	    borrowerName: string;
	    lentDate: string;
	    returnDate?: string;
	    notes?: string;
	
	    static createFrom(source: any = {}) {
	        return new LentCard(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.id = source["id"];
	        this.card = this.convertValues(source["card"], CardIdentity);
	        this.quantity = source["quantity"];
	        this.borrowerName = source["borrowerName"];
	        this.lentDate = source["lentDate"];
	        this.returnDate = source["returnDate"];
	        this.notes = source["notes"];
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}

}

export namespace collection {
	
	export class DeckCompareRow {
	    board?: string;
	    card: cards.CardIdentity;
	    needed: number;
	    owned: number;
	    missing: number;
	
	    static createFrom(source: any = {}) {
	        return new DeckCompareRow(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.board = source["board"];
	        this.card = this.convertValues(source["card"], cards.CardIdentity);
	        this.needed = source["needed"];
	        this.owned = source["owned"];
	        this.missing = source["missing"];
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class BuildDeckInput {
	    name: string;
	    replaceDeckId: number;
	    rows: DeckCompareRow[];
	
	    static createFrom(source: any = {}) {
	        return new BuildDeckInput(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.name = source["name"];
	        this.replaceDeckId = source["replaceDeckId"];
	        this.rows = this.convertValues(source["rows"], DeckCompareRow);
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class RepairCandidate {
	    fromOracleId: string;
	    toCard: cards.CardIdentity;
	
	    static createFrom(source: any = {}) {
	        return new RepairCandidate(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.fromOracleId = source["fromOracleId"];
	        this.toCard = this.convertValues(source["toCard"], cards.CardIdentity);
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class DeckCompareResult {
	    rows: DeckCompareRow[];
	    unresolved: string[];
	    repairs: RepairCandidate[];
	    hasUnresolved: boolean;
	
	    static createFrom(source: any = {}) {
	        return new DeckCompareResult(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.rows = this.convertValues(source["rows"], DeckCompareRow);
	        this.unresolved = source["unresolved"];
	        this.repairs = this.convertValues(source["repairs"], RepairCandidate);
	        this.hasUnresolved = source["hasUnresolved"];
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	
	export class ResolvedLine {
	    line: importer.ImportLine;
	    oracleId: string;
	    name: string;
	    scryfallUri: string;
	    source: string;
	
	    static createFrom(source: any = {}) {
	        return new ResolvedLine(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.line = this.convertValues(source["line"], importer.ImportLine);
	        this.oracleId = source["oracleId"];
	        this.name = source["name"];
	        this.scryfallUri = source["scryfallUri"];
	        this.source = source["source"];
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class ImportPreview {
	    validated: ResolvedLine[];
	    unresolved: string[];
	
	    static createFrom(source: any = {}) {
	        return new ImportPreview(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.validated = this.convertValues(source["validated"], ResolvedLine);
	        this.unresolved = source["unresolved"];
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	

}

export namespace importer {
	
	export class ImportLine {
	    raw: string;
	    quantity: number;
	    name: string;
	    scryfallId?: string;
	    board?: string;
	
	    static createFrom(source: any = {}) {
	        return new ImportLine(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.raw = source["raw"];
	        this.quantity = source["quantity"];
	        this.name = source["name"];
	        this.scryfallId = source["scryfallId"];
	        this.board = source["board"];
	    }
	}

}

export namespace storage {
	
	export class LendInput {
	    OracleID: string;
	    Quantity: number;
	    BorrowerName: string;
	    LentDate: string;
	    Notes: string;
	
	    static createFrom(source: any = {}) {
	        return new LendInput(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.OracleID = source["OracleID"];
	        this.Quantity = source["Quantity"];
	        this.BorrowerName = source["BorrowerName"];
	        this.LentDate = source["LentDate"];
	        this.Notes = source["Notes"];
	    }
	}

}

