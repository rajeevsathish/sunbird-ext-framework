import { Manifest, BaseServer } from '@project-sunbird/ext-framework-server/models';
import { Request, Response } from 'express';
import { FormResponse } from './models';
import * as _ from 'lodash';
import { telemetryHelper } from './telemetryHelper';
export class Server extends BaseServer {

  constructor(manifest: Manifest) {
    super(manifest);
  }
  private convertToLowerCase(obj: Object, keys: Array<string>){
      keys.forEach(element => obj[element] = obj[element] && obj[element].toLowerCase());
  }

  public async create(req: Request, res: Response) {
    const data = _.pick(req.body.request, ['type', 'subType', 'action', 'rootOrgId', 'framework', 'data', 'component']);
    this.convertToLowerCase(data, ['type', 'subType', 'action']);
    const model =  await this.getRowData(data);
    if(model && model.isDataExists) {
      this.createRow(data, req, res);
    } else {
      this.sendError(req, res, 'api.form.create', { code: 500, msg: 'Form Already Exists' });
    }
  }

  public async createRow(data, req: Request, res: Response, currentVersion?) {
    let version = 'v1';
    const versionhistory= '';
    if(currentVersion){
      let counter = Number(currentVersion.split("")[1]);
      version = 'v'+(counter+1)
    }
   const model =  new this.cassandra.instance.form_data({
      root_org: data.rootOrgId,
      type: data.type,
      subtype: data.subType,
      action: data.action,
      component: data.component,
      framework: data.framework,
      data: JSON.stringify(data.data),
      created_on: new Date(),
      isretired: 'false',
      version: version,
      versionhistory: versionhistory
    })
    await model.saveAsync().then(data => {
      res.status(200)
        .send(new FormResponse(undefined, {
          id: 'api.form.create',
          data: {
            created: 'OK'
          }
        }))
        telemetryHelper.log(req);
    })
      .catch(error => {
        res.status(500)
          .send(new FormResponse({
            id: "api.form.create",
            err: "ERR_CREATE_FORM_DATA",
            errmsg: error
          }));
        telemetryHelper.error(req, res, error);
      })
  }

  public async getRowData(data: any, addretirekey?) {
    const query = {
      root_org: data.rootOrgId || '*',
      framework: data.framework || '*',
      type: data.type,
      action: data.action,
      subtype: data.subType || '*',
      component: data.component || '*',
    }
    if(addretirekey){
    } else{
      query['isretired']= 'false'
    }
   return  await this.cassandra.instance.form_data.findAsync(query, {raw:true, allow_filtering: true}).then(async data => {
     const returnObj = {}
      if (!data) {
        returnObj['isDataExists'] = true
        return returnObj;
      } else {
        if(data.length === 0){
          returnObj['isDataExists'] = true
          return returnObj;
        } else {
         returnObj['isDataExists'] = false;
         returnObj['version'] = data[0].version;
         returnObj['versionhistory'] = data[0].versionhistory;
         returnObj['length'] = data.length;
         return returnObj;
        }
      }
    });
  }

  public async update(req: Request, res: Response) {
    const data = _.pick(req.body.request, ['type', 'subType', 'action', 'rootOrgId', 'framework', 'data', 'component']);
    this.convertToLowerCase(data, ['type', 'subType', 'action']);
    let query = {
      root_org: data.rootOrgId || '*',
      framework: data.framework || '*',
      type: data.type,
      action: data.action,
      subtype: data.subType || '*',
      component: data.component || '*',
    };

    const updateValue = {
      data: JSON.stringify(data.data),
      last_modified_on: new Date()
    };
    const model =  await this.getRowData(data);
    if (model && !model.isDataExists){
      data['version'] = model.version;
      this.retireAndCreateRow(data, req, res, updateValue);
    } else {
      this.sendError(req, res, 'api.form.update', { code: 500, msg: 'No Form to update' });
    }
  }

  public async retireAndCreateRow (data, req: Request, res: Response, updatedata){
     let version = 'v1'
     let versionhistory= '';
    if(data.version){
      let counter = Number(data.version.split("")[1]);
      version = 'v'+(counter+1)
    }
    let query = {
      root_org: data.rootOrgId || '*',
      framework: data.framework || '*',
      type: data.type,
      action: data.action,
      subtype: data.subType,
      component: data.component,
      version: data.version
    };

    const updateValue = {
      isretired: 'true',
      last_modified_on: new Date()
    };
    const model = new this.cassandra.instance.form_data({
      root_org: data.rootOrgId || '*',
      framework: data.framework || '*',
      type: data.type,
      action: data.action,
      subtype: data.subType || '*',
      component: data.component || '*',
      version : version,
      versionhistory : data.version,
      data: updatedata.data,
      created_on: new Date(),
      isretired: 'false',
    });
    
    await this.cassandra.instance.form_data.updateAsync(query, updateValue, { if_exists: true })
      .then(data => {
        if (!_.get(data, "rows[0].['[applied]']")) throw { msg: `invalid request, no records found for the match to retire!`, client_error: true };
        model.saveAsync().then(data => {
      res.status(200)
        .send(new FormResponse(undefined, {
          id: 'api.form.create',
          data: {
            created: 'OK'
          }
        }))
        telemetryHelper.log(req);
    })
      .catch(error => {
        res.status(500)
          .send(new FormResponse({
            id: "api.form.create",
            err: "ERR_CREATE_FORM_DATA",
            errmsg: error
          }));
        telemetryHelper.error(req, res, error);
      })
        
    }).catch(error => {
        if (error.client_error) {
          res.status(500)
            .send(new FormResponse({
              id: "api.form.retire",
              err: "ERR_RETIRE_FORM_DATA",
              responseCode: "CLIENT_ERROR",
              errmsg: error.msg
            }));
            telemetryHelper.error(req, res, error);
        } else {
          throw error;
        }
      })
      .catch(error => {
        res.status(404)
          .send(new FormResponse({
            id: "api.form.retire",
            err: "ERR_RETIRE_FORM_DATA",
            errmsg: error
          }));
        telemetryHelper.error(req, res, error);
      })
  }

  public async rowRetire (data, req: Request, res: Response){
    let query = {
      root_org: data.rootOrgId || '*',
      framework: data.framework || '*',
      type: data.type,
      action: data.action,
      subtype: data.subType,
      component: data.component,
      version: data.version
    };

    const updateValue = {
      isretired: 'true',
      last_modified_on: new Date()
    };
    
    await this.cassandra.instance.form_data.updateAsync(query, updateValue, { if_exists: true })
      .then(data => {
        if (!_.get(data, "rows[0].['[applied]']")) throw { msg: `invalid request, no records found for the match to retire!`, client_error: true };
        res.status(200)
          .send(new FormResponse(undefined, {
            id: 'api.form.retire',
            data: { "response": [{ "rootOrgId": query.root_org, "key": `${query.type}.${query.subtype}.${query.action}.${query.component}`, "status": "RETIRE_SUCCESS" }] }
          }))
        telemetryHelper.log(req);
      }).catch(error => {
        if (error.client_error) {
          res.status(500)
            .send(new FormResponse({
              id: "api.form.retire",
              err: "ERR_RETIRE_FORM_DATA",
              responseCode: "CLIENT_ERROR",
              errmsg: error.msg
            }));
            telemetryHelper.error(req, res, error);
        } else {
          throw error;
        }
      })
      .catch(error => {
        res.status(404)
          .send(new FormResponse({
            id: "api.form.retire",
            err: "ERR_RETIRE_FORM_DATA",
            errmsg: error
          }));
        telemetryHelper.error(req, res, error);
      })
  }

  public async read(req: Request, res: Response) {
    const data = _.pick(req.body.request, ['type', 'subType', 'action', 'rootOrgId', 'framework', 'data', 'component']);
    this.convertToLowerCase(data, ['type', 'subType', 'action']);
    const query = {
      root_org: data.rootOrgId || '*',
      framework: data.framework || '*',
      type: data.type,
      action: data.action,
      subtype: data.subType || '*',
      component: data.component || '*',
      isretired: 'false'
    }
    await this.cassandra.instance.form_data.findOneAsync(query, {raw:true, allow_filtering: true}).then(async data => {
      if (!data) {
        // find record by specified rootOrgId with framework = '*'
        await this.cassandra.instance.form_data.findOneAsync(Object.assign({}, query, { framework: "*" }), {raw:true, allow_filtering: true})
      } else {
        return data;
      }
    })
    .then(async data => {
        if (!data) {
          // get the default data
          return await this.cassandra.instance.form_data.findOneAsync(Object.assign({}, query, { root_org: "*" }), {raw:true, allow_filtering: true})
        } else {
          return data;
        }
      })
      .then(async data => {
        if (!data) {
          // get the default data
          return await this.cassandra.instance.form_data.findOneAsync(Object.assign({}, query, { root_org: "*", framework: "*" }), {raw:true, allow_filtering: true})
        } else {
          return data;
        }
      })
      .then(async data => {
        if (!data) {
          // get the default data
          return await this.cassandra.instance.form_data.findOneAsync(Object.assign({}, query, { root_org: "*", framework: "*", component: "*" }), {raw:true, allow_filtering: true})
        } else {
          return data;
        }
      })
      .then(data => {
        if (!data) {
          data = {};
        } 
        if (data && typeof data.data === "string") {
          data.data = JSON.parse(data.data);
        } 
        //data = data.toJSON(); // it removes all the schema validator of cassandra and gives plain object;
        if (_.get(data, 'root_org')) {
          data.rootOrgId = data.root_org;
          data = _.omit(data, ['root_org', 'version', 'isretired', 'versionhistory']);
        }
        res.status(200)
          .send(new FormResponse(undefined, {
            id: 'api.form.read',
            data: {
              form: data
            }
          }))
          telemetryHelper.log(req);
      })
      .catch(error => {
        res.status(404)
          .send(new FormResponse({
            id: "api.form.read",
            err: "ERR_READ_FORM_DATA",
            errmsg: error
          }));
        telemetryHelper.error(req, res, error);
      })
  }

  public async listAll(req: Request, res: Response) {
    const data = _.pick(req.body.request, ['type', 'subType', 'action', 'rootOrgId', 'framework', 'data', 'component']);
    const searchCriteria = ['type', 'subtype', 'action', 'root_org', 'framework', 'data', 'component', 'version', 'isretired'];
    
    const searchQuery = {
    };
    if(data.type){
      searchQuery['type'] = data.type;
    } 
    if(data.action){
      searchQuery['action'] = data.action;
    } 
    if(data.framework){
      searchQuery['framework'] = data.framework;
    } 
    if(data.subType){
      searchQuery['subtype'] = data.subType;
    } 
    if(data.component){
      searchQuery['component'] = data.component;
    } 
    if(data.rootOrgId){
      searchQuery['root_org'] = data.rootOrgId;
    }

    let formDetails;
    try {
      formDetails = await this.cassandra.instance.form_data.findAsync(searchQuery, { allow_filtering: true, select: searchCriteria, raw: true });
      const apiResponse = {
        forms: formDetails,
        count: _.get(formDetails, 'length')
      }
      this.sendSuccess(req, res, 'api.form.list', apiResponse);
    } catch (error) {
      let errorCode = "ERR_LIST_ALL_FORM";
      this.sendError(req, res, 'api.form.list', { code: errorCode, msg: error });
    }
  }

  public async retire(req: Request, res: Response) {
    const data = _.pick(req.body.request, ['type', 'subType', 'action', 'rootOrgId', 'framework', 'component', 'version']);
    this.convertToLowerCase(data, ['type', 'subType', 'action']);
    console.log('I am here and trying to retire the form');
    this.rowRetire(data, req, res);
  }

  public async restore(req: Request, res: Response) {
    const data = _.pick(req.body.request, ['type', 'subType', 'action', 'rootOrgId', 'framework', 'component', 'version']);
    this.convertToLowerCase(data, ['type', 'subType', 'action']);
    const rowData = await this.getRowData(data, false);
    if(rowData && !rowData.isDataExists) {
      if(rowData.length > 0) {
        this.sendError(req, res, 'api.form.restore', { code: 500, msg: 'Form cannot be restored there is a form active' });
      } else {
        console.log ('------->', rowData);
      }   
    } else{
        console.log ('------->', rowData);
      }  
  }

  private sendSuccess(req, res, id,  data){
    res.status(200)
      .send(new FormResponse(undefined, {
        id: id || 'api.list',
        data: data
      }))
    telemetryHelper.log(req);
  }

  private sendError(req, res,id, error){
    res.status(500)
      .send(new FormResponse({
        id: error.id || "api.list",
        err: error.code || "FORM_API_ERROR",
        errmsg: error.msg || "internal error"
      }));
    telemetryHelper.error(req, res, error);
  }
}