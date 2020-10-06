//-----------------------------------------------------------------------------
// © 2020 BlueVenn Limited
//-----------------------------------------------------------------------------
//
//	Component	:	BlueVenn.Indigo.DataManagement.Shared
//
//-----------------------------------------------------------------------------
//	Version History
//	---------------
//	Date		Author		Xref.		Notes
//	----		------		-----		-----
//	01/10/2020	J.Boyce					First release
//
//-----------------------------------------------------------------------------
// Namespace references
// --------------------
namespace BlueVenn.Indigo.DataManagement.Shared.Interfaces
{
	/// <summary>
	/// The external tables manager singleton
	/// </summary>
	/// <remarks>
	/// The external tables manager is a singleton used to auto update tasks on external tables
	/// </remarks>
	public interface IExternalTablesManager
	{
		/// <summary>
		/// Initialise the ExternalTablesManager
		/// </summary>
		/// <remarks>Performs initialisation of the ExternalTablesManager. This method is called just once during service startup.
		/// </remarks>
		void Initialise();
	}
}
