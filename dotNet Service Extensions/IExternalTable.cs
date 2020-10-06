//-----------------------------------------------------------------------------
// © 2017 BlueVenn Ltd
//-----------------------------------------------------------------------------
//
//	Component	:	BlueVenn.Indigo.DataManagement.Shared
//
//-----------------------------------------------------------------------------
//	Version History
//	---------------
//	Date		Author		Xref.		Notes
//	----		------		-----		-----
//	11/06/2018	J.Boyce		-			First release
//
//-----------------------------------------------------------------------------
// Namespace references
// --------------------
using System.Collections.Generic;
using BlueVenn.Indigo.DataAccess.Shared.Schema;
using BlueVenn.Indigo.ObjectManagement.Shared.Interfaces;
using BlueVenn.Indigo.ServerManagement.Shared.Interfaces;

namespace BlueVenn.Indigo.DataManagement.Shared.Interfaces
{
	/// <summary>
	/// Describes an external table object
	/// </summary>
	/// <remarks>
	/// External table objects allow customers to load bespoke data into their 
	/// analytic store in order to extend it
	/// </remarks>
	public interface IExternalTable: IObject, IExternalMappingObject, IExecutableTask
	{
		/// <summary>
		/// Gets or sets the table this external table is loading data into
		/// </summary>
		ObjectIdentity Table { get; set; }

		/// <summary>
		/// Gets or sets the table type
		/// </summary>
		EExternalTableType TableType { get; set; }

		/// <summary>
		/// Gets or sets the external resource object to use as an auto update source
		/// </summary>
		ObjectIdentity AutoUpdateSource { get; set; }

		/// <summary>
		/// Gets or sets the external resource object auto update source interval in minutes
		/// </summary>
		/// <remarks>
		/// Only applicable if an auto update source has been defined
		/// </remarks>
		int? Interval { get; set; }

		/// <summary>
		/// The last auto update event fetched by this external tables
		/// </summary>
		int? LastFetchedAutoUpdateEventId { get; set; }

		/// <summary>
		/// Gets a flag which indicates if the external table has data
		/// </summary>
		bool ExternalTableHasData { get; }

		/// <summary>
		/// Adds/Modifies/Deletes row data in an external table
		/// </summary>
		/// <param name="rawData">Raw JSON data to add/modify/delete</param>
		/// <param name="opToPerform">Add/Modify/Delete operation being performed</param>
		/// <returns>Number of rows processed</returns>
		int ProcessDataBlock( dynamic rawData, EExternalTableOperation opToPerform );

		/// <summary>
		/// Gets the current row count for the external table's staging table
		/// </summary>
		/// <returns>Number of rows in the staging table</returns>
		int GetRowCount();

		/// <summary>
		/// Helper to get the store name for external table
		/// </summary>
		/// <returns>Working store name</returns>
		string GetStoreName();

		/// <summary>
		/// Create the external table store
		/// </summary>
		void CreateStore();

		/// <summary>
		/// Get the campaign output fields for an external table
		/// </summary>
		/// <returns>List of fields to output</returns>
		List<BasicDataSetColumnInfo> GetOutputFields();

		/// <summary>
		/// Auto update an external table which has an auto-update source
		/// </summary>
		/// <returns>Number of records loaded</returns>
		int AutoUpdate();
	}
}
