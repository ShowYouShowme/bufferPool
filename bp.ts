import { MongoClient } from "mongodb";

class BufferPool {
    docs : Map<number, Object> = new Map();
    updateQueue : {uid : number, doc : Object}[] = [];
    insertQueue : {uid : number, doc : Object}[] = [];
    processing  : boolean  = false;  // 标记是否处于fsync状态
    readonly uri   : string = "mongodb://192.168.2.102:8000";
    readonly db    : string = "animal";
    readonly table : string = "sheeps";
    readonly cycleTime : number = 5000;
    private constructor() {
        this.load();
     }

    async load() {
        const client = new MongoClient(this.uri);
        try {
            const database = client.db(this.db);
            const collection = database.collection(this.table);
            const query = { };
            const cursor  = await collection.find(query);
            let arr = await cursor.toArray();
            for(let doc of arr){
                let UserID = doc['UserID'];
                if(UserID == undefined || Number.isInteger(UserID) == false)
                    throw '加载数据库到Cache时发生错误'
                this.docs.set(doc['UserID'], doc);
            }
        } finally {
            console.log(`数据加载完毕...`);
            await client.close();
            this.run();
        }
    }

    async fsync() {
        // if(this.processing){
        //     console.info(`有一个session正在fsync...`);
        //     return;
        // }
        // this.processing = true;
        console.info(`>>>>start fsync...`);
        const client = new MongoClient(this.uri);
        try {
            const database = client.db(this.db);
            const collection = database.collection(this.table);
            let operations  = [];
            let n1 = this.updateQueue.length;
            let n2 = this.insertQueue.length;
            if(n1 == 0 && n2 == 0){
                console.info(`没有需要保存的更新!`);
                return;
            }
            for(let elem of this.insertQueue){
                let {uid, doc} = elem;
                let operation = {
                    insertOne : {
                        document : doc
                    }
                };
                operations.push(operation);
            }

            for(let elem of this.updateQueue){
                let {uid, doc} = elem;
                let operation = {
                    replaceOne : {
                        filter : {_id : uid},
                        replacement : doc
                    }
                };
                operations.push(operation);
            }
            const result = await collection.bulkWrite(operations); 
            console.log(`result = ${JSON.stringify(result)}`);
            this.updateQueue.splice(0, n1);
            this.insertQueue.splice(0, n2);
        }catch(error){
            console.error(`fsync时发生错误, error = ${error}`);
        }
        finally {
            await client.close();
            // this.processing = false;
            console.info(`<<<<end fsync... \n`);
        }
    }

    after(ts : number){
        return new Promise((resolve, reject)=>{
            setTimeout(() => {
                resolve(null);
            }, ts);
        })
    }

    // 这么写,确保同时只能有一个fsync任务在执行
    async run() {
        while(true){
            console.log(`${(new Date()).toLocaleString()}`);
            await this.after(this.cycleTime);
            await this.fsync();
        }
    }

    findOne(uid: number) {
        let doc = this.docs.get(uid);
        if(doc == undefined)
            throw '用户不存在';
        return doc;
    }

    updateOne(uid: number, doc: any) {
        this.updateQueue.push({uid, doc});
        this.docs.set(uid, doc);
    }

    insertOne(uid: number, doc: any) {
        this.insertQueue.push({uid, doc});
        this.docs.set(uid, doc);
    }

    private static inst : BufferPool = new BufferPool();
    static getInstance(){
        return this.inst;
    }
}

// 产生测试数据
async function GenDocs(){
    const client = new MongoClient("mongodb://192.168.2.102:8000");
    try {
        const database = client.db("animal");
        const collection = database.collection("sheeps");
        let operations  = [];
        for(let i = 0; i < 15; ++i){
            let idx = 10000 + i;

            // doc的类型必须是any,否则bulkWrite 函数会提示错误
            let doc : any = {
                _id : idx,
                UserID : idx,
                NickName : `phone${idx}`, 
                Gold : 0,
                Avatar : `http://192.168.2.102:9002/source/img/head/sys_head_${idx}.png`,
                Safe : 0, 
                TigerTrainingDone : 1
            }
            let operation = {
                insertOne : {
                    document : doc
                }
            };
            operations.push(operation);
        }

        const result = await collection.bulkWrite(operations); 
        console.log(`result = ${JSON.stringify(result)}`);
    }catch(error){
        console.error(`fsync时发生错误, error = ${error}`);
    }
    finally {
        await client.close();
    }
}

// GenDocs();

// 测试数据变更
setInterval(()=>{
    let bp = BufferPool.getInstance();
    let doc : any = bp.findOne(10039);
    console.info(`doc = ${doc}`);

    doc['NickName'] = 'phone100035BBB';
    bp.updateOne(doc['UserID'], doc);
}, 5000);

