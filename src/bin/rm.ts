import { FileHandle } from "fs/promises";
import { OpenFileResult, saveHeaderAndPayload } from "./head";
import { findCollection } from "./data";
import { writeData } from "./utils";

export async function removeCollection(fd: FileHandle, result: OpenFileResult, collection: string) {
    const collectionMeta = findCollection(result, collection);
    if (!collectionMeta) throw new Error("Collection not found");

    if (result.options.overwriteRemovedCollection) {
        await writeData(fd, collectionMeta.offset, Buffer.alloc(collectionMeta.capacity), collectionMeta.capacity);
    }

    if (result.collections.length === 1) {
        result.collections = [];
        result.freeList = [];
        await fd.truncate(0);
        await saveHeaderAndPayload(fd, result);
        return;
    }

    result.collections.splice(result.collections.findIndex(c => c.name === collection), 1);
    result.freeList.push({
        offset: collectionMeta.offset,
        capacity: collectionMeta.capacity
    });
    await saveHeaderAndPayload(fd, result);
}