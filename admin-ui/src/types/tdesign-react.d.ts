declare module 'tdesign-react' {
  type ChangeHandler<T> = (value: T) => void;

  interface BaseProps {
    className?: string;
    style?: React.CSSProperties;
    children?: React.ReactNode;
    [key: string]: unknown;
  }

  interface InputProps extends BaseProps {
    value?: string | number;
    placeholder?: string;
    maxlength?: number;
    onChange?: ChangeHandler<string>;
    onEnter?: () => void;
  }

  interface TextareaProps extends BaseProps {
    value?: string;
    placeholder?: string;
    maxlength?: number;
    autosize?: { minRows?: number; maxRows?: number };
    onChange?: ChangeHandler<string>;
  }

  interface InputNumberProps extends BaseProps {
    value?: number | null;
    min?: number;
    max?: number;
    step?: number;
    placeholder?: string;
    onChange?: ChangeHandler<number | undefined>;
  }

  interface SwitchProps extends BaseProps {
    value?: boolean;
    onChange?: ChangeHandler<boolean>;
  }

  interface SelectOption {
    label: React.ReactNode;
    value: string | number;
    [key: string]: unknown;
  }

  interface SelectProps extends BaseProps {
    value?: string | number | Array<string | number>;
    options?: SelectOption[];
    placeholder?: string;
    onChange?: ChangeHandler<string | number | Array<string | number>>;
  }

  interface FormItemProps extends BaseProps {
    label?: React.ReactNode;
    help?: React.ReactNode;
    name?: string;
  }

  interface FormProps extends BaseProps {
    layout?: 'vertical' | 'inline';
    labelAlign?: 'left' | 'right' | 'top';
    colon?: boolean;
  }

  interface FormComponent {
    (props: FormProps): React.ReactElement | null;
    FormItem: (props: FormItemProps) => React.ReactElement | null;
  }

  interface ButtonProps extends BaseProps {
    theme?: 'default' | 'primary' | 'success' | 'warning' | 'danger';
    variant?: 'base' | 'outline' | 'dashed' | 'text' | 'fill';
    size?: 'small' | 'medium' | 'large';
    loading?: boolean;
    disabled?: boolean;
    block?: boolean;
    icon?: React.ReactNode;
    onClick?: () => void;
  }

  interface CardProps extends BaseProps {
    title?: React.ReactNode;
    bordered?: boolean;
    loading?: boolean;
  }

  interface SpaceProps extends BaseProps {
    direction?: 'horizontal' | 'vertical';
    size?: 'small' | 'medium' | 'large' | number;
    align?: 'start' | 'end' | 'center' | 'baseline';
    breakLine?: boolean;
  }

  interface TagProps extends BaseProps {
    theme?: 'default' | 'primary' | 'success' | 'warning' | 'danger';
    variant?: 'dark' | 'light' | 'outline' | 'light-outline';
  }

  interface LinkProps extends BaseProps {
    theme?: 'default' | 'primary' | 'success' | 'warning' | 'danger';
    hover?: 'color' | 'underline';
    onClick?: () => void;
  }

  interface AlertProps extends BaseProps {
    theme?: 'info' | 'success' | 'warning' | 'error';
    message?: React.ReactNode;
    description?: React.ReactNode;
    operation?: React.ReactNode;
    close?: React.ReactNode;
  }

  interface EmptyProps extends BaseProps {
    description?: React.ReactNode;
  }

  interface LoadingProps extends BaseProps {
    loading?: boolean;
    text?: React.ReactNode;
  }

  interface LayoutComponent {
    (props: BaseProps): React.ReactElement | null;
    Header: (props: BaseProps) => React.ReactElement | null;
    Aside: (props: BaseProps & { width?: string | number }) => React.ReactElement | null;
    Content: (props: BaseProps) => React.ReactElement | null;
  }

  interface MenuComponent {
    (props: BaseProps & { value?: string | number; onChange?: ChangeHandler<string | number> }): React.ReactElement | null;
    MenuItem: (props: BaseProps & { value?: string | number; icon?: React.ReactNode }) => React.ReactElement | null;
  }

  interface DescriptionItem {
    label: React.ReactNode;
    content: React.ReactNode;
    [key: string]: unknown;
  }

  interface DescriptionsProps extends BaseProps {
    items?: DescriptionItem[];
    column?: number;
    layout?: 'horizontal' | 'vertical';
  }

  interface TableCellContext<T> {
    row: T;
    col: { colKey?: string | number };
    rowIndex: number;
  }

  interface TableColumn<T> {
    colKey: string;
    title?: React.ReactNode;
    width?: string | number;
    ellipsis?: boolean;
    cell?: (context: TableCellContext<T>) => React.ReactNode;
    [key: string]: unknown;
  }

  interface TableProps<T extends object = object> extends BaseProps {
    rowKey?: string;
    size?: 'small' | 'medium' | 'large';
    hover?: boolean;
    data?: T[];
    columns?: TableColumn<T>[];
  }

  interface PaginationProps extends BaseProps {
    current?: number;
    pageSize?: number;
    total?: number;
    totalContent?: boolean | React.ReactNode;
    showPageSize?: boolean;
    disabled?: boolean;
    onChange?: (pageInfo: { current: number; previous: number; pageSize: number }) => void;
  }

  interface DialogButtonConfig {
    content?: React.ReactNode;
    theme?: 'default' | 'primary' | 'warning' | 'danger';
    loading?: boolean;
    disabled?: boolean;
    [key: string]: unknown;
  }

  interface DialogProps extends BaseProps {
    visible?: boolean;
    header?: React.ReactNode;
    confirmBtn?: React.ReactNode | DialogButtonConfig;
    cancelBtn?: React.ReactNode | DialogButtonConfig;
    onClose?: () => void;
    onConfirm?: () => void | Promise<void>;
    destroyOnClose?: boolean;
    width?: string | number;
  }

  interface DrawerProps extends BaseProps {
    visible?: boolean;
    header?: React.ReactNode;
    footer?: React.ReactNode | false;
    placement?: 'left' | 'right' | 'top' | 'bottom';
    size?: string | number;
    destroyOnClose?: boolean;
    onClose?: () => void;
  }

  interface DialogPluginOptions {
    header?: React.ReactNode;
    body?: React.ReactNode;
    confirmBtn?: React.ReactNode | DialogButtonConfig;
    cancelBtn?: React.ReactNode | DialogButtonConfig;
    theme?: 'default' | 'warning' | 'danger';
    onConfirm?: () => void | Promise<void>;
    onClose?: () => void;
  }

  interface DialogInstance {
    hide: () => void;
  }

  export const ConfigProvider: (props: BaseProps & { globalConfig?: unknown }) => React.ReactElement | null;
  export const Alert: (props: AlertProps) => React.ReactElement | null;
  export const Button: (props: ButtonProps) => React.ReactElement | null;
  export const Card: (props: CardProps) => React.ReactElement | null;
  export const Descriptions: (props: DescriptionsProps) => React.ReactElement | null;
  export const Dialog: (props: DialogProps) => React.ReactElement | null;
  export const Drawer: (props: DrawerProps) => React.ReactElement | null;
  export const Empty: (props: EmptyProps) => React.ReactElement | null;
  export const Form: FormComponent;
  export const Input: (props: InputProps) => React.ReactElement | null;
  export const InputNumber: (props: InputNumberProps) => React.ReactElement | null;
  export const Layout: LayoutComponent;
  export const Link: (props: LinkProps) => React.ReactElement | null;
  export const Loading: (props: LoadingProps) => React.ReactElement | null;
  export const Menu: MenuComponent;
  export const Pagination: (props: PaginationProps) => React.ReactElement | null;
  export const Select: (props: SelectProps) => React.ReactElement | null;
  export const Space: (props: SpaceProps) => React.ReactElement | null;
  export const Switch: (props: SwitchProps) => React.ReactElement | null;
  export const Table: <T extends object = object>(props: TableProps<T>) => React.ReactElement | null;
  export const Tag: (props: TagProps) => React.ReactElement | null;
  export const Textarea: (props: TextareaProps) => React.ReactElement | null;

  export const DialogPlugin: {
    confirm: (options: DialogPluginOptions) => DialogInstance;
  };
  export const MessagePlugin: {
    success: (options: React.ReactNode | { content?: React.ReactNode; duration?: number }) => void;
    error: (options: React.ReactNode | { content?: React.ReactNode; duration?: number }) => void;
    warning: (options: React.ReactNode | { content?: React.ReactNode; duration?: number }) => void;
  };
  export const NotificationPlugin: {
    warning: (options: { title?: React.ReactNode; content?: React.ReactNode; duration?: number }) => void;
  };
}

declare module 'tdesign-react/es/locale/zh_CN' {
  const zhCN: Record<string, unknown>;
  export default zhCN;
}
