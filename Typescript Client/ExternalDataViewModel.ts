//-----------------------------------------------------------------------------
// (c) 2020 BlueVenn ltd
//-----------------------------------------------------------------------------
//
//	Component	:	BlueVenn.Indigo.UI.DataManagement
//
//-----------------------------------------------------------------------------
//	Version History
//	---------------
//	Date		Author		Xref.		Notes
//	----		------		-----		-----
//	03/10/2020	E.Hall 	-			First release
//
//-----------------------------------------------------------------------------
// Dependencies/Imports/Exports
// ----------------------------

import ifwk = Indigo.Framework;
import iinf = Indigo.Infrastructure;
import ianls = Indigo.Analysis;
import idmg = Indigo.DataManagement;
import * as ko from "knockout";
import * as _ from "lodash";
import * as fwkEnums from "modules/framework/models/Enums";
import * as dmgEnums from "modules/datamanagement/models/Enums";
import { ExternalResourceViewModel } from "modules/datamanagement/viewmodels/ExternalResourceViewModel";
import { FieldMappingViewModel } from "modules/datamanagement/viewmodels/FieldMappingViewModel";
import { ObjectTypeGroups } from "modules/framework/models/ObjectTypeGroups";
import { MenuType } from "modules/framework/models/Enums";


export class ExternalDataViewModel extends ExternalResourceViewModel implements idmg.ViewModels.IExternalDataViewModel, ifwk.Menus.IMenuProvider
{

	/**
	* Ref to the table service
	*/
	private _tableService: idmg.Services.ITableService;

	/**
	 * Track table change
	 */
	private _lastTable: ifwk.Models.IObjectIdentity = null;

	/**
	 * The table level of the external data.
	 */
	public table: KnockoutObservable<ifwk.Models.IObjectIdentity> = ko.observable<ifwk.Models.IObjectIdentity>( null );

	/**
	 * Object filter for table picker
	 */
	public tablePickerObjectFilter: ifwk.Models.IObjectFilter = { includeTypes: [fwkEnums.ObjectType.Table], selectableTypes: [fwkEnums.ObjectType.Table], rootObjectType: fwkEnums.ObjectType.Database };

	/**
	 * Component name
	 */
	public componentName: string = "externaldata";

	/**
	 * Target table primary keys
	 */
	public targetPrimaryKeys = ko.observableArray<idmg.Models.IColumnInfo>();

	/**
	 * Constructs a new ViewModel
	 * @param objectSummary {IObjectSummaryViewModel} Object metadata
	 * @param application {IApplication} Application singleton
	 */
	constructor( objectSummary: ifwk.ViewModels.IObjectSummaryViewModel, application: iinf.IApplication )
	{
		super( objectSummary, application );

		this._tableService = this._container.resolve<idmg.Services.ITableService>( "ITableService" );
		this._subscriptions.push( this.table.subscribe( this.tableChanged ) );
		this._subscriptions.push( this.targetPrimaryKeys.subscribe( this.targetKeysChanged ) );

		this.viewMode( dmgEnums.ExternalDataViewMode[dmgEnums.ExternalDataViewMode.ExternalData_Designer] );
	}
		
	/**
	 * Sets the model from the details loaded
	 * @param model
	 */
	protected setFromModelDetails( model: ifwk.Models.IObjectSummary ): void
	{
		let details = <idmg.Models.IExternalDataDetails>model.details;
		this.table( details.table );
		super.setFromModelDetails( model );
	}

	/**
	 * Builds the dto to transfer data to the server
	 */
	protected buildDto(): ifwk.Models.IObjectSummary
	{
		let details = <idmg.Models.IExternalDataDetails> super.buildDto().details;
		details.table = this.table();
		this.model.details = details;
		return _.cloneDeep( this.model );
	}

	/**
	* Handler for external resource changed event
	*/
	private tableChanged = ( newTable: ifwk.Models.IObjectIdentity ) =>
	{
		if( newTable && !_.isEqual(this._lastTable, newTable) )
		{
			this._lastTable = newTable;

			// Fetch primary keys
			this._tableService.getPrimaryKeys( newTable.id ).then( keyInfo =>
			{
				this.targetPrimaryKeys( keyInfo );
			} ).catch( error =>
			{
				this.showErrors( error );
			} ).done();
		}
	}

	/**
	* Handler for target keys changed event
	*/
	private targetKeysChanged = ( keysSchema: idmg.Models.IColumnInfo[] ) =>
	{
		let prevMappings = this.fieldMappings();

		if( keysSchema )
		{
			// Check whether we already have mappings for these keys
			let needsUpdate = false;
			keysSchema.forEach( ( key, i ) =>
			{
				if( needsUpdate )
					return;

				if( key.tableName !== this.table().name )
					needsUpdate = true;

				if( key.name !== ( prevMappings[i] && prevMappings[i].targetFieldId() && prevMappings[i].targetFieldId().name ) )
					needsUpdate = true;
			} );

			if( !needsUpdate )
				return;

			this.setDefaultMappings();
		}
		else
		{
			this.fieldMappings( [] );
		}

		if( prevMappings )
			prevMappings.forEach( mapping => mapping.disposeExplicit() );
	}

	/** 
	 * Sets field mappings to initial default
	 * Sets up mappings based on the primary keys of the selected table
	 * */
	protected setDefaultMappings()
	{
		let keysSchema = this.targetPrimaryKeys();

		if( !keysSchema )
			return;

		// Add the primary key mappings
		let mappings = _.map( keysSchema, col =>
		{
			// Find the field object 
			if( col.tableName === this.table().name )
			{
				// Find all fields with name containing col.name ...
				let matchingFields = this._objectStore.getObjects( { rootIdentity: this.table, includeTypes: ObjectTypeGroups.fields, matchTerm: col.name }, true )();
				// Filter to ensure the name is an exact match
				matchingFields = _.filter( matchingFields, ( f ) => { return f.name() === col.name; } );
				// Found it?
				if( matchingFields && matchingFields.length === 1 )
					return new FieldMappingViewModel( this._objectStore, matchingFields[0].getIdentity(), false, null, this.sourceSchema, this._locale );
			}

			return null;
		} );

		// Add the data value mapping
		mappings.push( new FieldMappingViewModel( this._objectStore, null, true, null, this.sourceSchema, this._locale ) );

		mappings = _.filter( mappings, m => !!m );

		this.fieldMappings( mappings );
	}

}