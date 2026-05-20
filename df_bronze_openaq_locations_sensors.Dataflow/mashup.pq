[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[Description = "API-Key for OPENAQ"]
shared OPENAQKEY = "" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type any, DefaultValue = ""];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "location_date_sensor_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "location_id", DestinationColumnName = "location_id"], [SourceColumnName = "name", DestinationColumnName = "name"], [SourceColumnName = "locality", DestinationColumnName = "locality"], [SourceColumnName = "timezone", DestinationColumnName = "timezone"], [SourceColumnName = "code", DestinationColumnName = "code"], [SourceColumnName = "latitude", DestinationColumnName = "latitude"], [SourceColumnName = "longitude", DestinationColumnName = "longitude"], [SourceColumnName = "sensor_id", DestinationColumnName = "sensor_id"], [SourceColumnName = "p_name", DestinationColumnName = "p_name"], [SourceColumnName = "p_units", DestinationColumnName = "p_units"], [SourceColumnName = "p_display_name", DestinationColumnName = "p_display_name"], [SourceColumnName = "first_datetime_utc", DestinationColumnName = "first_datetime_utc"], [SourceColumnName = "last_datetime_utc", DestinationColumnName = "last_datetime_utc"], [SourceColumnName = "provider_name", DestinationColumnName = "provider_name"], [SourceColumnName = "loaded_at", DestinationColumnName = "loaded_at"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared location_date_sensor = let
    ApiKey = OPENAQKEY,
    BaseUrl = "https://api.openaq.org",

    GetPage = (page as number) =>
        let
            Source = Web.Contents(
                BaseUrl,
                [
                    RelativePath = "v3/locations",
                    Query = [
                        bbox = "-74.26,40.49,-73.69,40.92",
                        limit = "1000",
                        page = Number.ToText(page)
                    ],
                    Headers = [
                        #"X-API-Key" = ApiKey,
                        #"Accept" = "application/json"
                    ]
                ]
            ),
            Json = Json.Document(Source)
        in
            Json,

    Pages = List.Generate(
        () => [page = 1, data = GetPage(1)],
        each List.Count(Record.FieldOrDefault([data], "results", {})) > 0,
        each [page = [page] + 1, data = GetPage([page] + 1)],
        each [data]
    ),
    AllResults = List.Combine(List.Transform(Pages, each Record.Field(_, "results"))),
    AsTable = Table.FromList(AllResults, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    Expanded = Table.ExpandRecordColumn(AsTable, "Column1", 
        {"id", "name", "locality", "timezone", "country", "coordinates", "sensors", "datetimeFirst", "datetimeLast", "provider"}),
  #"Expanded country" = Table.ExpandRecordColumn(Expanded, "country", {"code"}, {"code"}),
  #"Expanded coordinates" = Table.ExpandRecordColumn(#"Expanded country", "coordinates", {"latitude", "longitude"}, {"latitude", "longitude"}),
  #"Expanded datetimeFirst" = Table.ExpandRecordColumn(#"Expanded coordinates", "datetimeFirst", {"utc"}, {"first_datetime_utc"}),
  #"Expanded datetimeLast" = Table.ExpandRecordColumn(#"Expanded datetimeFirst", "datetimeLast", {"utc"}, {"last_datetime_utc"}),
  #"Expanded provider" = Table.ExpandRecordColumn(#"Expanded datetimeLast", "provider", {"name"}, {"provider_name"}),
  #"Expanded sensors" = Table.ExpandListColumn(#"Expanded provider", "sensors"),
  #"Expanded sensors 2" = Table.ExpandRecordColumn(#"Expanded sensors", "sensors", {"id", "parameter"}, {"sensor_id", "parameter"}),
  #"Expanded parameter" = Table.ExpandRecordColumn(#"Expanded sensors 2", "parameter", {"name", "units", "displayName"}, {"p_name", "p_units", "p_display_name"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded parameter", {{"id", Int64.Type}, {"sensor_id", Int64.Type}, {"name", type text}, {"locality", type text}, {"timezone", type text}, {"code", type text}, {"p_name", type text}, {"p_units", type text}, {"p_display_name", type text}, {"provider_name", type text}, {"latitude", type number}, {"longitude", type number}, {"first_datetime_utc", type datetime}, {"last_datetime_utc", type datetime}}),
  Custom = Table.RenameColumns(#"Changed column type",{{"id", "location_id"}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(Custom, "loaded_at", each DateTime.LocalNow()), {{"loaded_at", type datetime}})
in
    #"Added custom";
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "2e0a9a0f-a9ac-4770-9137-10b52d0b6df6"]}[Data]{[lakehouseId = "c4c9cf64-1667-4152-bd26-12854382cfbe"]}[Data];
shared location_date_sensor_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true]),
  Navigation_1 = Pattern{[workspaceId = "2e0a9a0f-a9ac-4770-9137-10b52d0b6df6"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "c4c9cf64-1667-4152-bd26-12854382cfbe"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "openaq_locations", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
