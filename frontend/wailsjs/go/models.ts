export namespace cards {
	
	export class CardIdentity {
	    oracleId: string;
	    name: string;
	    scryfallUri: string;
	
	    static createFrom(source: any = {}) {
	        return new CardIdentity(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.oracleId = source["oracleId"];
	        this.name = source["name"];
	        this.scryfallUri = source["scryfallUri"];
	    }
	}
	export class CollectionItem {
	    card: CardIdentity;
	    quantity: number;
	    lentQty: number;
	    inDeck: boolean;
	    available: number;
	
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
	
	    static createFrom(source: any = {}) {
	        return new DeckCard(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.card = this.convertValues(source["card"], CardIdentity);
	        this.quantity = source["quantity"];
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
	    card: cards.CardIdentity;
	    needed: number;
	    owned: number;
	    missing: number;
	
	    static createFrom(source: any = {}) {
	        return new DeckCompareRow(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
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
	    Name: string;
	    ReplaceDeckID: number;
	    Rows: DeckCompareRow[];
	
	    static createFrom(source: any = {}) {
	        return new BuildDeckInput(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.Name = source["Name"];
	        this.ReplaceDeckID = source["ReplaceDeckID"];
	        this.Rows = this.convertValues(source["Rows"], DeckCompareRow);
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
	
	    static createFrom(source: any = {}) {
	        return new ImportLine(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.raw = source["raw"];
	        this.quantity = source["quantity"];
	        this.name = source["name"];
	        this.scryfallId = source["scryfallId"];
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

