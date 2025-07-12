import { ValtheraClass } from "@wxn0brp/db-core";
import { BinFileAction } from "./actions";
import { BinManager, Options } from "./bin";

export * from "./bin";
export * from "./actions";

export async function createBinValthera(path: string, opts: Partial<Options> = {}, init = true) {
    const mgr = new BinManager(path, opts);
    const actions = new BinFileAction(mgr);
    const db = new ValtheraClass({ dbAction: actions });

    if (init) await actions.init();

    return {
        db,
        actions,
        mgr,
    }
}