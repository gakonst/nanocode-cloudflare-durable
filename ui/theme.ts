import type { Theme } from '@pierre/highlights'

export const themes = {
  dark: theme({
    comment: '#6c6a66',
    constant: '#ffcf8a',
    fg: '#e8e6e1',
    function: '#3fd7e8',
    keyword: '#ff6a2b',
    name: 'nanocode-dark',
    string: '#9ad8d2',
    type: 'dark',
    types: '#ffb27a',
  }),
  light: theme({
    comment: '#8a8578',
    constant: '#a15c00',
    fg: '#141414',
    function: '#0a7c8c',
    keyword: '#d9480f',
    name: 'nanocode-light',
    string: '#23676f',
    type: 'light',
    types: '#9a3d00',
  }),
}

function theme(options: theme.Options): Theme {
  return {
    appearance: options.type,
    name: options.name,
    style: {
      'editor.background': 'transparent',
      'editor.foreground': options.fg,
      syntax: {
        comment: { color: options.comment, font_style: 'italic' },
        constant: options.constant,
        constructor: options.types,
        function: options.function,
        keyword: options.keyword,
        number: options.constant,
        property: options.types,
        string: options.string,
        type: options.types,
      },
    },
  }
}

declare namespace theme {
  type Options = {
    comment: string
    constant: string
    fg: string
    function: string
    keyword: string
    name: string
    string: string
    type: 'dark' | 'light'
    types: string
  }
}
