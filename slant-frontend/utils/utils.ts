import { BACKEND_URL } from '@/constants/constants';

const generateUrl = (url: string) => {
    return `${BACKEND_URL}${url}`;
}

export { generateUrl };