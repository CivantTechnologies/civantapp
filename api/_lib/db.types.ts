export type Json =
  | string
  | number
  | boolean
  | null
  | { [key: string]: Json | undefined }
  | Json[];

type TableDef<Row, Insert = Row, Update = Partial<Insert>> = {
  Row: Row;
  Insert: Insert;
  Update: Update;
  Relationships: [];
};

export type ApiDatabase = {
  public: {
    Tables: {
      tenants: TableDef<
        {
          id: string;
          name: string;
          regions: string[] | null;
          plan: string | null;
          plan_limits: Json | null;
          limits: Json | null;
          created_at: string | null;
        },
        {
          id: string;
          name: string;
          regions?: string[] | null;
          plan?: string | null;
          plan_limits?: Json | null;
          limits?: Json | null;
          created_at?: string | null;
        }
      >;
      users: TableDef<
        {
          id: string;
          email: string;
          tenant_id: string;
          role: string | null;
          created_at: string | null;
        },
        {
          id: string;
          email: string;
          tenant_id: string;
          role?: string | null;
          created_at?: string | null;
        }
      >;
      user_roles: TableDef<
        {
          user_id: string;
          role: string;
        },
        {
          user_id: string;
          role: string;
        }
      >;
      ConnectorConfig: TableDef<
        {
          id: string;
          connector_key: string | null;
          connector_id: string | null;
          source: string | null;
          tenant_id: string | null;
          enabled: boolean | null;
          status: string | null;
          schedule: string | null;
          fetch_interval: string | null;
          updated_at: string | null;
          created_at: string | null;
          config: Json | null;
        },
        {
          id?: string;
          connector_key?: string | null;
          connector_id?: string | null;
          source?: string | null;
          tenant_id?: string | null;
          enabled?: boolean | null;
          status?: string | null;
          schedule?: string | null;
          fetch_interval?: string | null;
          updated_at?: string | null;
          created_at?: string | null;
          config?: Json | null;
        }
      >;
      ConnectorRuns: TableDef<
        {
          id: string;
          connector_key: string | null;
          connector_id: string | null;
          source: string | null;
          tenant_id: string | null;
          status: string | null;
          started_at: string | null;
          finished_at: string | null;
          error_summary: string | null;
          metadata: Json | null;
          created_at: string | null;
        },
        {
          id?: string;
          connector_key?: string | null;
          connector_id?: string | null;
          source?: string | null;
          tenant_id?: string | null;
          status?: string | null;
          started_at?: string | null;
          finished_at?: string | null;
          error_summary?: string | null;
          metadata?: Json | null;
          created_at?: string | null;
        }
      >;
      support_access_grants: TableDef<
        {
          id: string;
          tenant_id: string;
          support_user_id: string | null;
          enabled: boolean;
          expires_at: string | null;
          enabled_by_user_id: string | null;
          reason: string | null;
          created_at: string | null;
          revoked_at: string | null;
          revoked_by_user_id: string | null;
          revoke_reason: string | null;
        },
        {
          id: string;
          tenant_id: string;
          support_user_id?: string | null;
          enabled?: boolean;
          expires_at?: string | null;
          enabled_by_user_id?: string | null;
          reason?: string | null;
          created_at?: string | null;
          revoked_at?: string | null;
          revoked_by_user_id?: string | null;
          revoke_reason?: string | null;
        }
      >;
      support_access_audit: TableDef<
        {
          id: string;
          tenant_id: string;
          actor_user_id: string;
          actor_email: string | null;
          action: string;
          reason: string | null;
          metadata_json: Json | null;
          created_at: string | null;
        },
        {
          id: string;
          tenant_id: string;
          actor_user_id: string;
          actor_email?: string | null;
          action: string;
          reason?: string | null;
          metadata_json?: Json | null;
          created_at?: string | null;
        }
      >;
    };
    Views: Record<string, never>;
    Functions: Record<string, never>;
    Enums: Record<string, never>;
    CompositeTypes: Record<string, never>;
  };
};
