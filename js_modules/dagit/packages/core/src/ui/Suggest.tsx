import {IPopoverProps} from '@blueprintjs/core';
// eslint-disable-next-line no-restricted-imports
import {Suggest as BlueprintSuggest, SuggestProps} from '@blueprintjs/select';
import deepmerge from 'deepmerge';
import * as React from 'react';

export const SuggestWIP = <T extends unknown>(props: React.PropsWithChildren<SuggestProps<T>>) => {
  const popoverProps: Partial<IPopoverProps> = {
    ...props.popoverProps,
    minimal: true,
    modifiers: deepmerge(
      {offset: {enabled: true, offset: '0, 8px'}},
      props.popoverProps?.modifiers || {},
    ),
    popoverClassName: `dagit-popover ${props.popoverProps?.className || ''}`,
  };

  return <BlueprintSuggest {...props} popoverProps={popoverProps} />;
};
