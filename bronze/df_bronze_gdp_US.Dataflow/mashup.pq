[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Query_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "indicator_id", DestinationColumnName = "indicator_id"], [SourceColumnName = "indicator_name", DestinationColumnName = "indicator_name"], [SourceColumnName = "country_id", DestinationColumnName = "country_id"], [SourceColumnName = "country_name", DestinationColumnName = "country_name"], [SourceColumnName = "countryiso3code", DestinationColumnName = "countryiso3code"], [SourceColumnName = "year", DestinationColumnName = "year"], [SourceColumnName = "value", DestinationColumnName = "value"], [SourceColumnName = "unit", DestinationColumnName = "unit"], [SourceColumnName = "obs_status", DestinationColumnName = "obs_status"], [SourceColumnName = "decimal", DestinationColumnName = "decimal"], [SourceColumnName = "loaded_at", DestinationColumnName = "loaded_at"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared Query = let
  Source = Json.Document(Web.Contents("https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD?format=json")),
  #"Converted to table" = Table.FromList(Source, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  Navigation = #"Converted to table"{1}[Column1],
  #"Converted to table 1" = Table.FromList(Navigation, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  #"Expanded Column1" = Table.ExpandRecordColumn(#"Converted to table 1", "Column1", {"indicator", "country", "countryiso3code", "date", "value", "unit", "obs_status", "decimal"}, {"indicator", "country", "countryiso3code", "year", "value", "unit", "obs_status", "decimal"}),
  #"Expanded indicator" = Table.ExpandRecordColumn(#"Expanded Column1", "indicator", {"id", "value"}, {"indicator_id", "indicator_name"}),
  #"Expanded country" = Table.ExpandRecordColumn(#"Expanded indicator", "country", {"id", "value"}, {"country_id", "country_name"}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Expanded country", "loaded_at", each DateTime.LocalNow()), {{"loaded_at", type datetime}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Added custom", {{"indicator_id", type text}, {"indicator_name", type text}, {"country_id", type text}, {"country_name", type text}, {"countryiso3code", type text}, {"unit", type text}, {"obs_status", type text}, {"year", Int64.Type}, {"value", Int64.Type}, {"decimal", Int64.Type}})
in
  #"Changed column type";
shared Query_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true]),
  Navigation_1 = Pattern{[workspaceId = "2e0a9a0f-a9ac-4770-9137-10b52d0b6df6"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "c4c9cf64-1667-4152-bd26-12854382cfbe"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "worldbank_gdp", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
