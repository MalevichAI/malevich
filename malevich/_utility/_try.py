from typing import Callable, TypeVar

ActionResult = TypeVar('ActionResult')
AttemptResult = TypeVar('AttemptResult')
FinalResult = TypeVar('FinalResult')

def try_cascade(
    action: Callable[[], ActionResult],
    *attempts: Callable[[Exception], AttemptResult],
    exceptions: tuple[Exception | KeyboardInterrupt] | None = None
) -> tuple[ActionResult | AttemptResult, Exception | KeyboardInterrupt | None]:
    if not exceptions:
        exceptions = (Exception,)
    try:
        result = action()
    except exceptions as e:
        for attempt in attempts:
            try:
                result = attempt(e)
                if result:
                    return action(), None
            except exceptions as e:
                pass
            else:
                return result, None
        return result, e
    else:
        return result, None
